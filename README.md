<div id="top"></div>

<p align="center">
<a href="https://github.com/kurtbuilds/tokio-cron/graphs/contributors">
    <img src="https://img.shields.io/github/contributors/kurtbuilds/tokio-cron.svg?style=flat-square" alt="GitHub Contributors" />
</a>
<a href="https://github.com/kurtbuilds/tokio-cron/stargazers">
    <img src="https://img.shields.io/github/stars/kurtbuilds/tokio-cron.svg?style=flat-square" alt="Stars" />
</a>
<a href="https://github.com/kurtbuilds/tokio-cron/actions">
    <img src="https://img.shields.io/github/actions/workflow/status/kurtbuilds/tokio-cron/test.yaml?style=flat-square" alt="Build Status" />
</a>
<a href="https://crates.io/crates/tokio-cron">
    <img src="https://img.shields.io/crates/d/tokio-cron?style=flat-square" alt="Downloads" />
</a>
<a href="https://crates.io/crates/tokio-cron">
    <img src="https://img.shields.io/crates/v/tokio-cron?style=flat-square" alt="Crates.io" />
</a>

</p>

# `tokio-cron`
`tokio-cron` is a simple cron-scheduler built on Tokio.

Why you might use it compared to alternatives:

- It's roughly 200 lines of code.
- It has support for `tracing`

Here is a 
```rust
use tokio_cron::{Scheduler, Job, daily};
use std::sync::atomic::{AtomicUsize, Ordering};

async fn simple_async_fn() {
    println!("Hello, world!");
}

async fn async_fn_with_args(counter: Arc<AtomicUsize>) {
    println!("Hello, world!");
}

#[tokio::main]
async fn main() {
    // You can use a local (timezone) scheduler, or a UTC scheduler.
    let mut scheduler = Scheduler::local();
    
    // This counter is to show data sharing in action. It's not required.
    // In a real environment, this might be a database connection pool, or other application state.
    let counter = Arc::new(AtomicUsize::new(0));

    // Add an async closure:
    // Run a named job "increase-counter" every day at 8am.
    let c = counter.clone();
    scheduler.add(Job::named("increase-counter", daily("8"), move || {
        let counter = c.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            println!("Hello, world!");
        }
    }));

    // Add a sync task:
    scheduler.add(Job::new_sync("*/1 * * * * *", move || {
        println!("Hello, world!");
    }));

    // Add a simple async function:
    scheduler.add(Job::new("*/1 * * * * *", simple_async_fn));
    
    // Add an async function with arguments:
    let c = counter.clone();
    scheduler.add(Job::new("*/2 * * * * *", move || {
        async_fn_with_args(c.clone())
    }));
    
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    
    let result = counter.clone().load(Ordering::SeqCst);
    println!("Counter: {}", result);
}
```

See tests in `src/lib.rs` for other examples and usage.