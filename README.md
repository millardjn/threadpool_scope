threadpool_scope
==============

A library for adding scopes to threadpools. Essentially [scoped_threadpool](https://crates.io/crates/scoped_threadpool), but adapted for use with [threadpool](https://crates.io/crates/threadpool).

For more details, see the [docs](https://docs.rs/threadpool_scope).

Note: the threadpool crate is expected to gain scope functionality in the next major version (2.0), which should obviate the need for this crate.

# Getting Started

[threadpool_scope is available on crates.io](https://crates.io/crates/threadpool_scope).
Add the following dependency to your Cargo manifest to get the latest version of the 0.1 branch:
```toml
[dependencies]
threadpool_scope = "0.1.*"
```

# Example

```rust
use threadpool::ThreadPool;
use threadpool_scope::scope_with;

fn main() {
    // Create a threadpool holding 4 threads
    let mut pool = ThreadPool::new(4);

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
```