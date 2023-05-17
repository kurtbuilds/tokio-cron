use std::collections::BinaryHeap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::str::FromStr;
use std::sync::Arc;
use chrono::{DateTime, Local, TimeZone, Utc};
use cron::Schedule;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{trace, info, error};
use futures::FutureExt;

type JobFunction = Arc<dyn Fn() + Send + Sync + 'static>;

struct InnerScheduler<T: TimeZone = Utc> {
    scheduled_jobs: RwLock<BinaryHeap<ScheduledJob<T>>>,
    notify: Notify,
    timezone: T,
}

#[derive(Clone)]
pub struct Scheduler<T: TimeZone = Utc> {
    inner: Arc<InnerScheduler<T>>,
}

impl Scheduler<Utc> {
    pub fn utc() -> Scheduler<Utc> {
        Scheduler::new_in_timezone(Utc)
    }

    pub fn local() -> Scheduler<Local> {
        Scheduler::new_in_timezone(Local)
    }
}

impl<Tz: TimeZone + Send + Sync + Debug + 'static> Scheduler<Tz>
    where
        Tz::Offset: Send + Sync,
{
    pub fn new_in_timezone(tz: Tz) -> Self {
        let r = Self {
            inner: Arc::new(InnerScheduler {
                scheduled_jobs: RwLock::new(BinaryHeap::new()),
                notify: Notify::new(),
                timezone: tz,
            }),
        };
        r.run();
        r
    }

    pub fn add(&mut self, job: Job) {
        let Job {
            cron_line,
            func,
            name,
        } = job;
        let cron = Schedule::from_str(&cron_line).unwrap();
        let job = ScheduledJob {
            dt: cron.upcoming(self.inner.timezone.clone()).next().unwrap(),
            cron,
            func,
            name,
        };
        let inner = self.inner.clone();
        tokio::spawn(async move {
            info!(name=%job.name, cron_line=?cron_line, first_dt=?job.dt, "Added job to cron tab");
            let mut queue = inner.scheduled_jobs.write()
                .await;
            queue.push(job);
            drop(queue);
            inner.notify.notify_one();
        });
    }

    fn run(&self) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            loop {
                let mut lock = inner.scheduled_jobs.write()
                    .await;
                let now = Utc::now();
                while let Some(next) = lock.peek() {
                    if next.dt > now {
                        break;
                    }
                    let to_run = lock.pop().unwrap();
                    let this = to_run.dt.clone();
                    let f = AssertUnwindSafe(to_run.func.clone());
                    let res = panic::catch_unwind(move || f());
                    if res.is_err() {
                        error!(name=%to_run.name, this_dt=?this, "Cron job panicked");
                    }
                    let next = to_run.next(inner.timezone.clone());
                    info!(name=%next.name, this_dt=?this, next_dt=?next.dt, "Ran job (Async job is running in background)");
                    lock.push(next);
                }
                let t = lock.peek().map(|s| s.dt.with_timezone(&Utc) - now).unwrap_or(DateTime::<Utc>::MAX_UTC - now);
                drop(lock);
                trace!(sec=t.num_seconds(), "Sleep cron main loop until timeout or added job");
                // timeout returns Err() if the timeout elapses, but that's our happy path
                let _ = timeout(t.to_std().unwrap(), inner.notify.notified()).await;
            }
        });
    }
}

struct ScheduledJob<T: TimeZone = Utc> {
    dt: DateTime<T>,
    cron: Schedule,
    func: JobFunction,
    name: String,
}

impl<Tz: TimeZone> ScheduledJob<Tz> {
    pub fn next(mut self, tz: Tz) -> Self {
        self.dt = self.cron.upcoming(tz).next().unwrap();
        self
    }
}

impl<Tz: TimeZone> PartialEq<ScheduledJob<Tz>> for ScheduledJob<Tz> {
    fn eq(&self, other: &Self) -> bool {
        self.dt.eq(&other.dt)
    }
}

impl<Tz: TimeZone> Eq for ScheduledJob<Tz> {}


impl<Tz: TimeZone> PartialOrd<ScheduledJob<Tz>> for ScheduledJob<Tz> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// std::collections::BinaryHeap is a max-heap,
/// so this reverses the ordering to make it a min-heap.
impl<Tz: TimeZone> Ord for ScheduledJob<Tz> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dt.cmp(&other.dt).reverse()
    }
}

impl Debug for ScheduledJob {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduledJob")
            .field("dt", &self.dt)
            .field("cron", &self.cron)
            .field("name", &self.name)
            .finish()
    }
}

pub struct Job {
    name: String,
    cron_line: String,
    func: JobFunction,
}

fn syncify_job<F, Fut>(name: &str, f: F) -> JobFunction
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output=()> + Send + 'static,
{
    let name = name.to_string();
    Arc::new(move || {
        let fut = f();
        let name = name.clone();
        tokio::spawn(async move {
            let res = AssertUnwindSafe(fut).catch_unwind().await;
            if res.is_err() {
                error!(name=%name, "Cron job panicked during async execution");
            }
        });
    })
}

impl Job {
    pub fn new<S, F, Fut>(cron: S, func: F) -> Self
        where
            F: Fn() -> Fut + Send + Sync + 'static,
            Fut: Future<Output=()> + Send + 'static,
            S: Into<String>,
    {
        Self {
            name: "".to_string(),
            cron_line: cron.into(),
            func: syncify_job("", func),
        }
    }

    /// Schedule a job that is not async
    pub fn new_sync<S, F>(cron: S, func: F) -> Self
        where
            F: Fn() -> () + Send + Sync + 'static,
            S: Into<String> {
        Self {
            name: "".to_string(),
            cron_line: cron.into(),
            func: Arc::new(func),
        }
    }

    /// Useful for debugging
    pub fn named<S, F, Fut>(name: &str, cron: S, func: F) -> Self
        where
            F: Fn() -> Fut + Send + Sync + 'static,
            Fut: Future<Output=()> + Send + 'static,
            S: Into<String>,
    {
        Self {
            name: name.to_string(),
            cron_line: cron.into(),
            func: syncify_job(name, func),
        }
    }

    pub fn named_sync<S, F>(name: &str, cron: S, func: F) -> Self
        where
            F: Fn() -> () + Send + Sync + 'static,
            S: Into<String>,
    {
        Self {
            name: name.to_string(),
            cron_line: cron.into(),
            func: Arc::new(func),
        }
    }
}

/// Convenience method for creating a job that runs every **day** on the **hour_spec** provided.
/// second,minute = 0; hour = input; day,week,month,year = *
/// # Example
/// ```
/// use tokio_cron::daily;
/// assert_eq!(daily("6"), "0 0 6 * * * *", "Run at 6am every day.");
/// assert_eq!(daily("*/3"), "0 0 */3 * * * *", "Run at every third hour (3am, 6am, 9am, 12pm, 3pm, 6pm, 9pm, 12am) of every day.");
///
pub fn daily(hour_spec: &str) -> String {
    format!("0 0 {} * * * *", hour_spec)
}

/// Convenience method for creating a job that runs every **hour** on the **minute_spec** provided.
/// second = 0; minute = input; hour,day,week,month,year = *
/// # Example
/// ```
/// use tokio_cron::hourly;
/// assert_eq!(hourly("20"), "0 20 * * * * *", "Run at 20 minutes past the hour, every hour.");
/// assert_eq!(hourly("*/15"), "0 */15 * * * * *", "Run at 15, 30, 45, and 0 minutes past every hour.");
///
pub fn hourly(minute_spec: &str) -> String {
    format!("0 {} * * * * *", minute_spec)
}

/// Convenience method for creating a job that runs on the given days of week, at the given hour.
/// second,minute = 0; hour,week = input; day,month,year = *
/// # Example
/// ```
/// use tokio_cron::weekly;
/// assert_eq!(weekly("Mon,Wed,Fri", "8"), "0 0 8 * * Mon,Wed,Fri *", "Run at 8am on Mon, Wed, Fri.");
/// assert_eq!(weekly("0,6", "0"), "0 0 0 * * 0,6 *", "Run at 12am on Saturday (6) and Sunday (0). Colloquially, that's Friday and Saturday night.");
///
pub fn weekly(week_spec: &str, hour_spec: &str) -> String {
    format!("0 0 {hour_spec} * * {week_spec} *")
}

/// Convenience method for creating a job that runs on the given days of week, at the given hour.
/// second,minute = 0; hour,day = input; month,week,year = *
/// # Example
/// ```
/// use tokio_cron::monthly;
/// assert_eq!(monthly("3", "8"), "0 0 8 3 * * *", "Run on 3rd of every month at 8am.");
///
pub fn monthly(day_spec: &str, hour_spec: &str) -> String {
    format!("0 0 {hour_spec} {day_spec} * * *")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn it_works() {
        tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::TRACE)
            .finish()
        ).unwrap();

        async fn async_func() {
            println!("Hello, world!");
        }

        let mut scheduler = Scheduler::local();
        let counter = Arc::new(AtomicUsize::new(0));

        // Add an async closure.
        // Explanation on the two clones:
        // First clone is because we need to move the counter into the closure.
        // Second clone is because the closure executes repeatedly, and each time, the closure
        // will need to own its own data. (hence clone every time the outside closure is called)
        let c = counter.clone();
        scheduler.add(Job::new("*/2 * * * * *", move || {
            let counter = c.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                println!("Hello, world!");
            }
        }));

        // Add a sync task.
        scheduler.add(Job::new_sync("*/1 * * * * *", move || {
            println!("Hello, world!");
        }));

        // Add an async function
        scheduler.add(Job::new("*/1 * * * * *", async_func));
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let result = counter.clone().load(Ordering::SeqCst);
        // Non-deterministic because we can have 1 or 2 executions of a */2 job in a 3 sec interval.
        assert!(result <= 2 && result >= 1);
    }

    #[tokio::test]
    async fn test_fancy() {
        async fn async_fn_with_args(counter: Arc<AtomicUsize>) {
            counter.fetch_add(1, Ordering::SeqCst);
        }

        let mut scheduler = Scheduler::local();
        let counter = Arc::new(AtomicUsize::new(0));

        scheduler.add(Job::named_sync("foo", hourly("1"), move || {
            println!("One minute into the hour!");
        }));

        scheduler.add(Job::named("foo", hourly("2"), move || {
            async move {
                println!("Two minutes into the hour!");
            }
        }));

        let c = counter.clone();
        scheduler.add(Job::named("increase-counter", "*/2 * * * * * *", move || {
            async_fn_with_args(c.clone())
        }));

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let result = counter.clone().load(Ordering::SeqCst);
        // Non-deterministic because we can have 1 or 2 executions of a */2 job in a 3 sec interval.
        assert!(result <= 2 && result >= 1);
    }

    #[tokio::test]
    async fn test_panic_doesnt_take_everything_down() {
        let mut scheduler = Scheduler::local();

        scheduler.add(Job::named_sync("causes-panic", "* * * * * * *", move || {
            panic!("This should not take down the scheduler!");
        }));

        scheduler.add(Job::named("panics-in-async", "* * * * * * *", move || {
            async move {
                panic!("This should not take down the scheduler!");
            }
        }));

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
