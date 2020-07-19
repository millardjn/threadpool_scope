//! This crate provides an interface based on the `scoped-threadpool` crate adapted for use with the `threadpool` crate's `ThreadPool`
//!
//! It can be used to execute a number of short-lived jobs in parallel
//! without the need to respawn the underlying threads.
//!
//! Jobs are runnable by borrowing the pool for a given scope, during which
//! an arbitrary number of them can be executed. These jobs can access data of
//! any lifetime outside of the pools scope, which allows working on
//! non-`'static` references in parallel.
//!
//! For safety reasons, a panic inside a worker thread will not be isolated,
//! but rather propagate to the outside of the pool.
//!
//! # Examples:
//!
//! ```
//! use threadpool::ThreadPool;
//! use threadpool_scope::scope_with;
//!
//! fn main() {
//!     // Create a threadpool holding 4 threads
//!     let pool = ThreadPool::new(4);
//!
//!     let mut vec = vec![0, 1, 2, 3, 4, 5, 6, 7];
//!
//!     // Use the threads as scoped threads that can
//!     // reference anything outside this closure
//!     scope_with(&pool, |scope| {
//!         // Create references to each element in the vector ...
//!         for e in &mut vec {
//!             // ... and add 1 to it in a seperate thread
//!             scope.execute(move || {
//!                 *e += 1;
//!             });
//!         }
//!     });
//!
//!     assert_eq!(vec, vec![1, 2, 3, 4, 5, 6, 7, 8]);
//! }
//! ```

use parking_lot::{Condvar, Mutex, MutexGuard};
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use threadpool::ThreadPool;

/// Borrows a threadpool creating a `Scope` which can be used to execute non-`'static` closures which may borrow from the scope of `scope_with`.
///
/// This is allowable as `scope_with` will not return until closures executed using the `Scope` are complete.
///
/// Note: By default the closure provided to `scope_with` may return much earlier than the closures it executes. If this is not desired call `join_all()` to force it to wait.
pub fn scope_with<'pool, 'scope, F, R>(pool: &'pool ThreadPool, f: F) -> R
where
    F: FnOnce(&Scope<'pool, 'scope>) -> R,
{
    let finished_mutex = Mutex::new(false);
    let mut finished_guard = unsafe {
        mem::transmute::<MutexGuard<'_, bool>, MutexGuard<'static, bool>>(finished_mutex.lock())
    };
    let scope = Scope {
        pool,
        _marker: PhantomData,
        state: ScopeState {
            finished_mutex: &finished_mutex as *const Mutex<bool>,
            finished_guard: &mut finished_guard as *mut MutexGuard<bool>,
            cvar: Condvar::new(),
            tokens_outstanding: AtomicUsize::new(1),
            workers_panicked: AtomicUsize::new(0),
        },
    };
    let x = f(&scope);
    scope.join_all();

    let workers_panicked = scope.state.workers_panicked.load(Ordering::Acquire);
    if workers_panicked > 0 {
        panic!("Worker thread panic count: {}", workers_panicked);
    }

    drop(scope);
    drop(finished_guard);
    x
}

type Thunk<'a> = Box<dyn FnOnce() + Send + 'a>;

pub struct Scope<'pool, 'scope> {
    pool: &'pool ThreadPool,
    _marker: PhantomData<::std::cell::Cell<&'scope mut ()>>,
    state: ScopeState,
}

/// The ScopeState contains the synchronisation primitives used to ensure the parent can join on the workers
///
/// The sequencing of actions on the atomics/mutex/guard/and condvar must ensure that:
///   1) the parent cant exit join_all while any workers are remaining
///      (either the parent must be the last to return its token or else it must wait for a signal through the mutex)
///   2) the parent cant be left waiting on the condvar because the workers all exit without notifying
///      (the parent always holding the guard except while waiting on the condvar, preventing the last worker notifying before the parent is waiting)
///      (if the parent wasnt the last to return its token then the last worker to finish will know it is the last and that it must signal through the mutex that is finished)
///   3) a worker cant be left trying to lock the mutex
///      (the parent will always wait on the condvar releasing the mutex if it was not the last token returned)
struct ScopeState {
    /// Virtual tokens are held by all workers still working, and by the parent at all times outside of the `join_all` method.
    /// When the parent returns its token, that signals its intent to sleep. If workers still remain this means the parent must be woken.
    /// If the parent token was the last to return, it can avoid sleeping as no workers remain to have received its intent.
    /// If any workers finish after this point one of them will know that it is the last and has the responsibility for waking the parent.
    /// This is reset to 1 at the end of the join_all method as we know the parent is the only token holder left.
    tokens_outstanding: AtomicUsize,
    workers_panicked: AtomicUsize,
    /// If the parent thread hands in its token before the workers finish, it waits on this mutex.
    /// Technically the bool isnt required on top of the notify signal as parking_lot doesnt have spurious wakes, but just in case things change.
    finished_mutex: *const Mutex<bool>,
    /// The scope holds a guard on the mutex at all times except when waiting on the condvar releases it.
    /// If this wasnt the case, there would be a datarace between the tokens atomic and the parent locking the mutex.
    finished_guard: *mut MutexGuard<'static, bool>,
    cvar: Condvar,
}

impl<'pool, 'scope> Scope<'pool, 'scope> {
    /// Execute a job on the threadpool.
    ///
    /// The body of the closure will be send to one of the
    /// internal threads, and this method itself will not wait
    /// for its completion.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'scope,
    {
        self.state.tokens_outstanding.fetch_add(1, Ordering::AcqRel);

        let mut s = Sentinel {
            successfully_finished: false,
            state: &self.state as *const _,
        };
        let b = unsafe {
            mem::transmute::<Thunk<'scope>, Thunk<'static>>(Box::new(move || {
                f();
                s.successfully_finished = true;
                drop(s);
            }))
        };

        self.pool.execute(b);
    }

    /// Blocks until all currently queued jobs have run to completion.
    ///
    /// This can be called at the end of the closure passed to `scope_with(..)` to ensure it does not return before the jobs it creates do.
    /// It may also be called during the closure passed to `scope_with(..)` multiple times to synchronise on the completions of batches of `Scope::execute(..)` calls.
    /// It will be automatically called just prior to `scope_with(..)` returning to ensure all jobs have completed.
    pub fn join_all(&self) {
        // We have guarenteed that this thread on this
        let finished_guard = unsafe { &mut *self.state.finished_guard };

        // If the parent returns its token before the last worker:
        //  * then it must wait on the condvar unlocking the mutex
        //  * the worker must lock the mutex to ensure the parent is waiting (this removes the raciness from the atomic to notify gap)
        //  * the worker must notify release the
        // If the last worker returns its token before the parent:
        //  * the worker can just exit
        //  * the parent will know it had the last token and can just exit the join_all without waiting on the condvar
        if self.state.tokens_outstanding.fetch_sub(1, Ordering::AcqRel) > 1 {
            while !**finished_guard {
                self.state.cvar.wait(finished_guard);
            }
        }
        // Setup for next time incase this is being called
        self.state.tokens_outstanding.store(1, Ordering::Release);
        **finished_guard = false;
    }
}

/// The `Sentinel` of each worker is responsible for returning its token, waking the parent thread if necessary, and notifying the parent if panics occur
/// For the life of a Sentinel, the parent Scope must always be active to avoid the state pointer becoming invalid
struct Sentinel {
    successfully_finished: bool,
    state: *const ScopeState,
}
unsafe impl Send for Sentinel {}

impl Drop for Sentinel {
    fn drop(&mut self) {
        unsafe {
            if !self.successfully_finished {
                (*self.state)
                    .workers_panicked
                    .fetch_add(1, Ordering::AcqRel);
            }

            let last_worker_and_parent_waiting = (*self.state)
                .tokens_outstanding
                .fetch_sub(1, Ordering::AcqRel)
                == 1;

            if last_worker_and_parent_waiting {
                let mut finished_guard = (&*(*self.state).finished_mutex).lock();
                *finished_guard = true;
                let _threadcount = (*self.state).cvar.notify_one();
                drop(finished_guard);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![cfg_attr(feature = "nightly", allow(unused_unsafe))]

    use super::scope_with;
    use num_cpus;
    use rayon;
    use scoped_threadpool::Pool;
    use std::sync;
    use std::thread;
    use std::time::{self, Instant};
    use threadpool::ThreadPool;

    fn sleep_ms(ms: u64) {
        thread::sleep(time::Duration::from_millis(ms));
    }

    #[test]
    fn threadpool_scope_sync_perf() {
        let pool = ThreadPool::new(num_cpus::get());

        let mut acc = (0..10_000).collect::<Vec<_>>();
        //let mut acc2 = (0..10_000).collect::<Vec<_>>();

        let durations = (0..1000)
            .map(|_| {
                let start = Instant::now();
                scope_with(&pool, |scope| {
                    for x in &mut acc {
                        scope.execute(move || {
                            *x += 1;
                        })
                    }
                    // scope.join_all();
                    // for x in &mut acc2 {
                    //     scope.execute(move ||{
                    //         *x += 1;
                    //     })
                    // }
                });
                start.elapsed().as_micros()
            })
            .collect::<Vec<_>>();

        println!("{:#?}", durations);
    }

    #[test]
    fn scope_threadpool_sync_perf() {
        let mut pool = Pool::new(num_cpus::get() as u32);

        let mut acc = (0..10_000).collect::<Vec<_>>();
        //let mut acc2 = (0..10_000).collect::<Vec<_>>();

        let durations = (0..1000)
            .map(|_| {
                let start = Instant::now();
                pool.scoped(|scope| {
                    for x in &mut acc {
                        scope.execute(move || {
                            *x += 1;
                        })
                    }
                    // scope.join_all();
                    // for x in &mut acc2 {
                    //     scope.execute(move ||{
                    //         *x += 1;
                    //     })
                    // }
                });
                start.elapsed().as_micros()
            })
            .collect::<Vec<_>>();

        println!("{:#?}", durations);
    }

    #[test]
    fn rayon_sync_perf() {
        let mut acc = (0..10_000).collect::<Vec<_>>();
        //let mut acc2 = (0..10_000).collect::<Vec<_>>();

        let durations = (0..1000)
            .map(|_| {
                let start = Instant::now();
                rayon::scope(|scope| {
                    for x in &mut acc {
                        scope.spawn(move |_| {
                            *x += 1;
                        })
                    }
                });
                // rayon::scope(|scope|{
                //     for x in &mut acc2 {
                //         scope.spawn(move |_|{
                //             *x += 1;
                //         })
                //     }
                // });
                start.elapsed().as_micros()
            })
            .collect::<Vec<_>>();

        println!("{:#?}", durations);
    }

    #[test]
    fn doctest() {
        // Create a threadpool holding 4 threads
        let pool = ThreadPool::new(4);

        let mut vec = vec![0, 1, 2, 3, 4, 5, 6, 7];

        // Use the threads as scoped threads that can
        // reference anything outside this closure
        scope_with(&pool, |scope| {
            // Create references to each element in the vector ...
            for e in &mut vec {
                // ... and add 1 to it in a seperate thread
                scope.execute(move || {
                    *e += 1;
                });
            }
        });

        assert_eq!(vec, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn smoketest() {
        let pool = ThreadPool::new(4);

        for i in 1..7 {
            let mut vec = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            scope_with(&pool, |s| {
                for e in vec.iter_mut() {
                    s.execute(move || {
                        *e += i;
                    });
                }
            });

            let mut vec2 = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            for e in vec2.iter_mut() {
                *e += i;
            }

            assert_eq!(vec, vec2);
        }
    }

    #[test]
    #[should_panic]
    fn thread_panic() {
        let pool = ThreadPool::new(4);
        scope_with(&pool, |scoped| {
            scoped.execute(move || panic!());
        });
    }

    #[test]
    #[should_panic]
    fn scope_panic() {
        let pool = ThreadPool::new(4);
        scope_with(&pool, |_scoped| panic!());
    }

    #[test]
    #[should_panic]
    fn pool_panic() {
        let _pool = ThreadPool::new(4);
        panic!()
    }

    #[test]
    fn join_all() {
        let pool = ThreadPool::new(4);

        let (tx_, rx) = sync::mpsc::channel();

        scope_with(&pool, |scoped| {
            let tx = tx_.clone();
            scoped.execute(move || {
                sleep_ms(1000);
                tx.send(2).unwrap();
            });

            let tx = tx_.clone();
            scoped.execute(move || {
                tx.send(1).unwrap();
            });

            scoped.join_all();

            let tx = tx_.clone();
            scoped.execute(move || {
                tx.send(3).unwrap();
            });
        });

        assert_eq!(rx.iter().take(3).collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn join_all_with_thread_panic() {
        use std::sync::mpsc::Sender;
        struct OnScopeEnd(Sender<u8>);
        impl Drop for OnScopeEnd {
            fn drop(&mut self) {
                self.0.send(1).unwrap();
                sleep_ms(200);
            }
        }
        let (tx_, rx) = sync::mpsc::channel();
        // Use a thread here to handle the expected panic from the pool. Should
        // be switched to use panic::recover instead when it becomes stable.
        let handle = thread::spawn(move || {
            let pool = ThreadPool::new(8);
            let _on_scope_end = OnScopeEnd(tx_.clone());
            scope_with(&pool, |scoped| {
                scoped.execute(move || {
                    sleep_ms(100);
                    panic!();
                });
                for _ in 1..8 {
                    let tx = tx_.clone();
                    scoped.execute(move || {
                        sleep_ms(200);
                        tx.send(0).unwrap();
                    });
                }
            });
        });
        if let Ok(..) = handle.join() {
            panic!("Pool didn't panic as expected");
        }
        // If the `1` that OnScopeEnd sent occurs anywhere else than at the
        // end, that means that a worker thread was still running even
        // after the `scoped` call finished, which is unsound.
        let values: Vec<u8> = rx.into_iter().collect();
        assert_eq!(&values[..], &[0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn safe_execute() {
        let pool = ThreadPool::new(4);
        scope_with(&pool, |scoped| {
            scoped.execute(move || {});
        });
    }
}

#[cfg(all(test, feature = "nightly"))]
mod benches {
    extern crate test;

    use self::test::{black_box, Bencher};
    use std::sync::Mutex;
    use threadpool::ThreadPool;

    // const MS_SLEEP_PER_OP: u32 = 1;

    lazy_static! {
        static ref POOL_1: Mutex<Pool> = Mutex::new(Pool::new(1));
        static ref POOL_2: Mutex<Pool> = Mutex::new(Pool::new(2));
        static ref POOL_3: Mutex<Pool> = Mutex::new(Pool::new(3));
        static ref POOL_4: Mutex<Pool> = Mutex::new(Pool::new(4));
        static ref POOL_5: Mutex<Pool> = Mutex::new(Pool::new(5));
        static ref POOL_8: Mutex<Pool> = Mutex::new(Pool::new(8));
    }

    fn fib(n: u64) -> u64 {
        let mut prev_prev: u64 = 1;
        let mut prev = 1;
        let mut current = 1;
        for _ in 2..(n + 1) {
            current = prev_prev.wrapping_add(prev);
            prev_prev = prev;
            prev = current;
        }
        current
    }

    fn threads_interleaved_n(pool: &mut Pool) {
        let size = 1024; // 1kiB

        let mut data = vec![1u8; size];
        pool.scoped(|s| {
            for e in data.iter_mut() {
                s.execute(move || {
                    *e += fib(black_box(1000 * (*e as u64))) as u8;
                    for i in 0..10000 {
                        black_box(i);
                    }
                    //sleep_ms(MS_SLEEP_PER_OP);
                });
            }
        });
    }

    #[bench]
    fn threads_interleaved_1(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_1.lock().unwrap()))
    }

    #[bench]
    fn threads_interleaved_2(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_2.lock().unwrap()))
    }

    #[bench]
    fn threads_interleaved_4(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_4.lock().unwrap()))
    }

    #[bench]
    fn threads_interleaved_8(b: &mut Bencher) {
        b.iter(|| threads_interleaved_n(&mut POOL_8.lock().unwrap()))
    }

    fn threads_chunked_n(pool: &mut Pool) {
        // Set this to 1GB and 40 to get good but slooow results
        let size = 1024 * 1024 * 10 / 4; // 10MiB
        let bb_repeat = 50;

        let n = pool.thread_count();
        let mut data = vec![0u32; size];
        pool.scoped(|s| {
            let l = (data.len() - 1) / n as usize + 1;
            for es in data.chunks_mut(l) {
                s.execute(move || {
                    if es.len() > 1 {
                        es[0] = 1;
                        es[1] = 1;
                        for i in 2..es.len() {
                            // Fibonnaci gets big fast,
                            // so just wrap around all the time
                            es[i] = black_box(es[i - 1].wrapping_add(es[i - 2]));
                            for i in 0..bb_repeat {
                                black_box(i);
                            }
                        }
                    }
                    //sleep_ms(MS_SLEEP_PER_OP);
                });
            }
        });
    }

    #[bench]
    fn threads_chunked_1(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_1.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_2(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_2.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_3(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_3.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_4(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_4.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_5(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_5.lock().unwrap()))
    }

    #[bench]
    fn threads_chunked_8(b: &mut Bencher) {
        b.iter(|| threads_chunked_n(&mut POOL_8.lock().unwrap()))
    }
}
