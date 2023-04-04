#![allow(unused)]

use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use chrono::{DateTime, Local, TimeZone, Utc};
use cron::Schedule;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};

type JobFunction = Arc<dyn Fn() + Send + Sync + 'static>;

struct InnerScheduler<T: TimeZone = Utc> {
    scheduled_jobs: RwLock<BTreeMap<DateTime<T>, ScheduledJob>>,
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

impl<Tz: TimeZone + Send + Sync + 'static> Scheduler<Tz>
    where
        Tz::Offset: Send + Sync,
{
    pub fn new_in_timezone(tz: Tz) -> Self {
        let r = Self {
            inner: Arc::new(InnerScheduler {
                scheduled_jobs: RwLock::new(BTreeMap::new()),
                notify: Notify::new(),
                timezone: tz,
            }),
        };
        r.run();
        r
    }

    pub fn add(&mut self, job: Job) {
        let scheduled_job = ScheduledJob {
            cron: Schedule::from_str(&job.cron_line).unwrap(),
            func: job.func,
        };
        let dt = scheduled_job.cron.upcoming(self.inner.timezone.clone()).next().unwrap();
        let inner = self.inner.clone();
        tokio::spawn(async move {
            println!("Added job to queue");
            let mut queue = inner.scheduled_jobs.write()
                .await;
            queue.insert(dt, scheduled_job);
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
                while let Some((dt, _)) = lock.first_key_value() {
                    if *dt > now {
                        break;
                    }
                    let (_, to_run) = lock.pop_first().unwrap();
                    (to_run.func)();
                    let next = to_run.cron.upcoming(inner.timezone.clone()).next().unwrap();
                    lock.insert(next, to_run);
                }
                let t = lock.first_key_value().map(|(dt, _)| dt.with_timezone(&Utc) - now).unwrap_or(DateTime::<Utc>::MAX_UTC - now);
                drop(lock);
                println!("Waiting for {} seconds...", t.num_seconds());
                timeout(t.to_std().unwrap(), inner.notify.notified()).await;
            }
        });
    }
}

struct ScheduledJob {
    cron: Schedule,
    func: JobFunction,
}

impl std::fmt::Debug for ScheduledJob {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduledJob")
            .field("cron", &self.cron)
            .finish()
    }
}

pub struct Job {
    cron_line: String,
    func: JobFunction,
}

impl Job {
    pub fn new<F, Fut>(cron: &str, func: F) -> Self
        where
            F: Fn() -> Fut + Send + Sync + 'static,
            Fut: Future<Output=()> + Send + 'static
    {
        Self {
            cron_line: cron.to_string(),
            func: Arc::new(move || {
                let fut = func();
                tokio::spawn(fut);
            }),
        }
    }

    /// Schedule a job that is not async
    pub fn new_sync<T>(cron: &str, func: T) -> Self
        where
            T: Fn() -> () + Send + Sync + 'static {
        Self {
            cron_line: cron.to_string(),
            func: Arc::new(func),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn it_works() {
        let mut scheduler = Scheduler::local();
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let counter = counter.clone();
            scheduler.add(Job::new("*/2 * * * * *", move || {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    println!("Hello, world!");
                }
            }));
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let result = counter.clone().load(Ordering::SeqCst);
        assert!(result <= 2);
    }
}
