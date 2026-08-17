use chrono::Utc;
use std::collections::{HashMap, HashSet, BinaryHeap};
use tokio::sync::Semaphore;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;
use serde_json::json;

use crate::file_store::FileStore;
use crate::rate_limiter::RateLimiter;
use crate::task::{RetryableTask, PriorityTask, ProgressMessage};
use tokio::sync::broadcast;

pub type TaskHandler = Arc<dyn Fn(String) -> Result<(), String> + Send + Sync>;
pub type MaxRetryHandler = Arc<dyn Fn(String) -> Result<(), String> + Send + Sync>;

struct ExecutingGuard {
    executing_tasks: Arc<Mutex<HashSet<String>>>,
    task_id: String,
}

impl Drop for ExecutingGuard {
    fn drop(&mut self) {
        if let Ok(mut executing) = self.executing_tasks.lock() {
            executing.remove(&self.task_id);
        }
    }
}

#[derive(Clone)]
pub struct SnerdQueue {
    pub name: String,
    pub file_store: FileStore,
    pub rate_limiter: RateLimiter,
    task_handlers: Arc<RwLock<HashMap<String, TaskHandler>>>,
    max_retry_handlers: Arc<RwLock<HashMap<String, MaxRetryHandler>>>,
    active_hashes: Arc<Mutex<HashSet<String>>>,
    executing_tasks: Arc<Mutex<HashSet<String>>>,
    worker_semaphore: Arc<Semaphore>,
    pub progress_tx: broadcast::Sender<ProgressMessage>,
}

impl SnerdQueue {
    pub fn new(name: &str, file_store: FileStore, rate_limiter: RateLimiter) -> Self {
        let mut initial_hashes = HashSet::new();
        if let Ok(tasks) = file_store.read_tasks() {
            for task in tasks {
                if task.deleted_at.is_none() {
                    if let Some(hash) = task.payload_hash {
                        initial_hashes.insert(hash);
                    }
                }
            }
        }

        let (progress_tx, _) = broadcast::channel(1024);
        Self {
            name: name.to_string(),
            file_store,
            rate_limiter,
            task_handlers: Arc::new(RwLock::new(HashMap::new())),
            max_retry_handlers: Arc::new(RwLock::new(HashMap::new())),
            active_hashes: Arc::new(Mutex::new(initial_hashes)),
            executing_tasks: Arc::new(Mutex::new(HashSet::new())),
            worker_semaphore: Arc::new(Semaphore::new(100)), // Limit to 100 concurrent tasks
            progress_tx,
        }
    }

    pub fn subscribe_progress(&self) -> broadcast::Receiver<ProgressMessage> {
        self.progress_tx.subscribe()
    }

    pub fn yield_progress(&self, task_id: &str, data: &str) {
        let _ = self.progress_tx.send(ProgressMessage {
            task_id: task_id.to_string(),
            data: data.to_string(),
        });
    }

    pub async fn register_task_handler<F>(&self, task_type: &str, handler: F)
    where
        F: Fn(String) -> Result<(), String> + Send + Sync + 'static,
    {
        self.task_handlers
            .write()
            .await
            .insert(task_type.to_string(), Arc::new(handler));
    }

    pub async fn register_max_retry_handler<F>(&self, task_type: &str, handler: F)
    where
        F: Fn(String) -> Result<(), String> + Send + Sync + 'static,
    {
        self.max_retry_handlers
            .write()
            .await
            .insert(task_type.to_string(), Arc::new(handler));
    }

    pub fn enqueue(&self, mut task: RetryableTask) -> std::io::Result<()> {
        if let Some(ref hash) = task.payload_hash {
            if let Ok(mut hashes) = self.active_hashes.lock() {
                if hashes.contains(hash) {
                    return Ok(());
                }
                hashes.insert(hash.clone());
            }
        }
        task.deleted_at = None;
        self.file_store.save_task(&task)?;

        if task.execute_at <= Utc::now() && task.retry_after_time <= Utc::now() {
            if let Some(ref group) = task.rate_limit_group {
                if let Some(limit) = task.max_per_minute {
                    match self.rate_limiter.check_and_increment(group, limit) {
                        Ok(true) => {}
                        Ok(false) | Err(_) => {
                            task.retry_after_time = Utc::now() + chrono::Duration::seconds(60);
                            let _ = self.file_store.save_task(&task);
                            return Ok(());
                        }
                    }
                }
            }

            // Lock check before executing
            if let Ok(mut executing) = self.executing_tasks.lock() {
                if executing.contains(&task.task_id) {
                    return Ok(());
                }
                executing.insert(task.task_id.clone());
            }

            let q = self.clone();
            tokio::spawn(async move {
                q.execute_task(task).await;
            });
        }
        Ok(())
    }

    pub async fn start_processor(&self, interval: Duration) {
        let q = self.clone();
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                q.process_due_tasks().await;
            }
        });
    }

    pub async fn process_due_tasks(&self) {
        let tasks = match self.file_store.read_tasks() {
            Ok(t) => t,
            Err(_) => return,
        };

        let now = Utc::now();
        let mut heap = BinaryHeap::new();
        
        for task in tasks {
            if task.execute_at <= now && task.retry_after_time <= now && task.deleted_at.is_none() {
                heap.push(PriorityTask(task));
            }
        }

        let available = self.worker_semaphore.available_permits();
        for _ in 0..available {
            if let Some(PriorityTask(mut task)) = heap.pop() {
                if let Some(ref group) = task.rate_limit_group {
                    if let Some(limit) = task.max_per_minute {
                        match self.rate_limiter.check_and_increment(group, limit) {
                            Ok(true) => {}
                            Ok(false) | Err(_) => {
                                task.retry_after_time = now + chrono::Duration::seconds(60);
                                let _ = self.file_store.save_task(&task);
                                continue;
                            }
                        }
                    }
                }

                // Lock check before executing
                if let Ok(mut executing) = self.executing_tasks.lock() {
                    // Double-check against the latest state in the file store to avoid TOCTOU race conditions
                    if let Ok(Some(latest_task)) = self.file_store.get_latest_task(&task.task_id) {
                        if latest_task.execute_at > now || latest_task.retry_after_time > now || latest_task.deleted_at.is_some() {
                            continue;
                        }
                    } else {
                        // Task was deleted completely
                        continue;
                    }

                    if executing.contains(&task.task_id) {
                        continue;
                    }
                    executing.insert(task.task_id.clone());
                }

                if let Ok(permit) = self.worker_semaphore.clone().try_acquire_owned() {
                    let q = self.clone();
                    tokio::spawn(async move {
                        let _p = permit;
                        q.execute_task(task).await;
                    });
                }
            } else {
                break;
            }
        }
    }

    async fn execute_task(&self, mut task: RetryableTask) {
        // Drop guard guarantees removal from executing_tasks
        let _guard = ExecutingGuard {
            executing_tasks: Arc::clone(&self.executing_tasks),
            task_id: task.task_id.clone(),
        };

        // Build the execution result — either via webhook HTTP call or local handler
        let result: Result<(), String> = if let Some(ref url) = task.webhook_url.clone() {
            // --- Webhook Path ---
            let payload = json!({
                "taskId": task.task_id,
                "taskType": task.task_type,
                "data": task.task_data,
            });
            let url = url.clone();
            tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();
                rt.block_on(async {
                    let mut client_builder = reqwest::Client::builder();
                    if let Some(secs) = task.max_execution_seconds {
                        client_builder = client_builder.timeout(std::time::Duration::from_secs(secs));
                    }
                    let client = client_builder.build().unwrap_or_else(|_| reqwest::Client::new());
                    
                    match client
                        .post(&url)
                        .header("Content-Type", "application/json")
                        .header("X-SnerdMQ-Event", "Execute")
                        .json(&payload)
                        .send()
                        .await
                    {
                        Ok(resp) if resp.status().is_success() => Ok(()),
                        Ok(resp) => Err(format!("Webhook returned non-2xx status: {}", resp.status())),
                        Err(e) => {
                            if e.is_timeout() {
                                Err(format!("Webhook execution timed out after {} seconds", task.max_execution_seconds.unwrap_or(0)))
                            } else {
                                Err(format!("Webhook request failed: {}", e))
                            }
                        }
                    }
                })
            })
            .await
            .unwrap_or_else(|e| Err(format!("Webhook task panic: {:?}", e)))
        } else {
            // --- Local Handler Path ---
            let handler = {
                let handlers = self.task_handlers.read().await;
                handlers.get(&task.task_type).cloned()
            };
            if let Some(h) = handler {
                let task_data = task.task_data.clone();
                let fut = tokio::task::spawn_blocking(move || h(task_data));
                
                if let Some(secs) = task.max_execution_seconds {
                    match tokio::time::timeout(std::time::Duration::from_secs(secs), fut).await {
                        Ok(Ok(res)) => res,
                        Ok(Err(e)) => Err(format!("Task panic: {:?}", e)),
                        Err(_) => Err(format!("Task execution timed out after {} seconds", secs)),
                    }
                } else {
                    fut.await.unwrap_or_else(|e| Err(format!("Task panic: {:?}", e)))
                }
            } else {
                return; // No handler and no webhook — nothing to do
            }
        };

        match result {
            Ok(_) => {
                let mut rescheduled = false;
                if let Some(ref cron_expr) = task.cron_expression {
                    use cron::Schedule;
                    use std::str::FromStr;
                    if let Ok(schedule) = Schedule::from_str(cron_expr) {
                        if let Some(next) = schedule.upcoming(Utc).next() {
                            task.execute_at = next;
                            task.retry_count = 0;
                            task.last_error_obj = None;
                            task.last_job_error = None;
                            let _ = self.file_store.save_task(&task);
                            rescheduled = true;
                        }
                    }
                }

                if !rescheduled {
                    let _ = self.file_store.delete_task(&task.task_id);
                    if let Some(ref hash) = task.payload_hash {
                        if let Ok(mut hashes) = self.active_hashes.lock() {
                            hashes.remove(hash);
                        }
                    }
                }
            }
            Err(e) => {
                if task.retry_count < task.max_retries {
                    task.update_retry_config(Some(e));
                    let _ = self.file_store.save_task(&task);
                } else {
                    // Max retries reached — fire DLQ webhook or local max retry handler
                    if let Some(ref url) = task.webhook_url.clone() {
                        let payload = json!({
                            "taskId": task.task_id,
                            "taskType": task.task_type,
                            "data": task.task_data,
                        });
                        let url = url.clone();
                        tokio::spawn(async move {
                            let _ = reqwest::Client::new()
                                .post(&url)
                                .header("Content-Type", "application/json")
                                .header("X-SnerdMQ-Event", "MaxRetriesReached")
                                .json(&payload)
                                .send()
                                .await;
                        });
                    } else {
                        let max_handler = {
                            let max_handlers = self.max_retry_handlers.read().await;
                            max_handlers.get(&task.task_type).cloned()
                        };
                        if let Some(mh) = max_handler {
                            let max_data = task.task_data.clone();
                            let _ = tokio::task::spawn_blocking(move || mh(max_data)).await;
                        }
                    }

                    let _ = self.file_store.delete_task(&task.task_id);
                    if let Some(ref hash) = task.payload_hash {
                        if let Ok(mut hashes) = self.active_hashes.lock() {
                            hashes.remove(hash);
                        }
                    }
                }
            }
        }
    }
}
