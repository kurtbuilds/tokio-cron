#![allow(unused)]

use std::collections::BTreeMap;
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
}

#[derive(Clone)]
pub struct Scheduler<T: TimeZone = Utc> {
    inner: Arc<InnerScheduler<T>>,
    timezone: T,
}

impl Scheduler<Utc> {
    pub fn utc() -> Scheduler<Utc> {
        Scheduler::new_in_timezone(Utc)
    }

    pub fn local() -> Scheduler<Local> {
        Scheduler::new_in_timezone(Local)
    }
}

impl<Tz: TimeZone + Send + 'static> Scheduler<Tz>
    where
        Tz::Offset: Send + Sync,
{
    pub fn new_in_timezone(tz: Tz) -> Self {
        let r = Self {
            inner: Arc::new(InnerScheduler {
                scheduled_jobs: RwLock::new(BTreeMap::new()),
                notify: Notify::new(),
            }),
            timezone: tz,
        };
        r.run();
        r
    }

    pub fn add(&mut self, job: Job) {
        let scheduled_job = ScheduledJob {
            cron: Schedule::from_str(&job.cron_line).unwrap(),
            func: job.func,
        };
        let dt = scheduled_job.cron.upcoming(self.timezone.clone()).next().unwrap();
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut queue = inner.scheduled_jobs.write()
                .await;
            queue.insert(dt, scheduled_job);
            drop(queue);
            inner.notify.notify_one();
        });
    }

    fn run(&self) {
        let inner = self.inner.clone();
        // what do we want to do here? peek at the first item in inner_schedule_jobs.
        // if its less than or equal to the current time, then execute the job.
        // otherwise, await inner.notified.timeout the next time we should run.
        // tokio::spawn(async move {
        //     loop {
        //         let queue = inner.scheduled_jobs.write()
        //             .await;
        //         let mut queue = queue.into_iter().peekable();
        //         let now = Utc::now();
        //         while let Some((dt, _)) = queue.peek() {
        //             if *dt > now {
        //                 break;
        //             }
        //             let (_, to_run) = queue.next().unwrap();
        //             (to_run.func)();
        //         }
        //         let t = queue.peek().map(|(dt, _)| dt.with_timezone(&Utc) - now).unwrap_or(DateTime::<Utc>::MAX_UTC - now);
        //         drop(queue);
        //         timeout(t.to_std().unwrap(), inner.notify.notified()).await;
        //     }
        // });
    }
}

struct ScheduledJob {
    cron: Schedule,
    func: JobFunction,
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

    #[tokio::test]
    async fn it_works() {
        let mut scheduler = Scheduler::local();
        let counter = Arc::new(AtomicUsize::new(0));
        scheduler.add(Job::new("*/2 * * * * *", move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                println!("Hello, world!");
            }
        }));
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let result = counter.load(Ordering::SeqCst);
        assert_eq!(result, 2);
    }
}
