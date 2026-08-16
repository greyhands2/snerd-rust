<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="Snerd-Rust Logo" />
  <h1>⚙️ snerd-rust v0.2.1</h1>
  <p>A blazingly fast, brutally simple, zero-dependency async background job engine for Rust.</p>

  [![Crates.io](https://img.shields.io/crates/v/snerd-rust.svg)](https://crates.io/crates/snerd-rust)
  [![Documentation](https://docs.rs/snerd-rust/badge.svg)](https://docs.rs/snerd-rust)
  [![CI](https://github.com/greyhands2/snerd-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/greyhands2/snerd-rust/actions/workflows/ci.yml)
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
</div>

If you are tired of wrestling with heavy, bloated background job frameworks like Redis, Postgres tables, or RabbitMQ just to send a few emails in the background... well, you are in the right place. 

`snerd-rust` is an embedded, high-performance background task queue that lives entirely in a single, perfectly OS-locked, append-only `.log` file on your file system. It was designed to bring the aggressive concurrency and lightweight footprint of Golang's `snerd` over to Rust's heavily optimized asynchronous ecosystem.

No databases. No external daemons. No nonsense.

---

## 🔥 v0.2.1 AI Features
* **Zero External Infrastructure**: You don't need a Redis cluster. Your tasks are persisted directly to `.snerdata/tasks/tasks.log` using standard filesystem I/O.
* **Bulletproof File Locks**: Safely scales across multiple processes! We utilize OS-level file-locking boundaries (`flock`) to guarantee that your tasks are never corrupted, even if multiple instances of your app try to write simultaneously.
* **Smart API Rate-Limiting**: Natively tracks `rate_limit_group` execution velocity to prevent 429 "Too Many Requests" API errors.
* **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
* **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
* **Asynchronous Tokio Core**: Built natively on top of `tokio`. Background workers process the queue without starving your main event loop.
* **Dead-Letter Queue (DLQ)**: Built-in `maxRetries` limits and hooks to elegantly catch and bury poison-pill tasks.

---

## 📦 Installation

Just add `snerd-rust` to your `Cargo.toml`:

```toml
[dependencies]
snerd-rust = "0.2.1"
```

*Note: You will also need `tokio` (with full features) since snerd is entirely async.*

---

## 🚀 Quickstart

It takes roughly 3 lines of code to spin up a queue and start firing background jobs. 

```rust
use snerd_rust::queue::SnerdQueue;
use snerd_rust::file_store::FileStore;
use snerd_rust::task::RetryableTask;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Advanced: Distributed Scaling
    // Point the embedded file store to your mounted shared network drive (e.g. AWS EFS)
    let storage_path = "/mnt/aws-efs-shared-drive/snerd_tasks.log";
    
    // Or, for local single-server storage:
    // let storage_path = ".snerdata/tasks/tasks.log";

    // 1. Initialize the Persistence Store
    let file_store = FileStore::new(storage_path).unwrap();
    
    // 2. Create the Queue
    let queue = SnerdQueue::new("my-fast-queue", file_store);

    // 3. Register your Task Handler (The closure that does the actual work)
    queue.register_task_handler("generate_ai_image", |data| {
        println!("Generating with payload: {}", data);
        // ... do your heavy lifting here!
        Ok(()) // Return Err("...") to trigger a retry!
    }).await;
    
    // 4. (Optional) Register a Dead-Letter Handler for when retries run out
    queue.register_max_retry_handler("generate_ai_image", |data| {
        println!("Task permanently failed! Payload: {}", data);
        Ok(())
    }).await;

    // 5. Boot the background processor polling loop
    queue.start_processor(Duration::from_secs(2)).await;

    // 6. Enqueue a task!
    let task = RetryableTask::new(
        "unique-task-id-123".to_string(), // ID
        "generate_ai_image".to_string(),  // Type (matches handler)
        r#"{"prompt": "A crab in space"}"#.to_string(), // JSON Payload
        3,    // Max retries
        1.0,  // Delay in hours for retries
        Some("openai_api".to_string()), // rate_limit_group
        Some(50),                       // max_per_minute
        Some(true),                     // auto_dedupe
        Some(0.95),                     // urgency_score
        None,                           // execute_at
        Some("1h".to_string()),         // cron: Runs every 1 hour!
        Some("https://api.example.com/webhook".to_string()), // webhook_url
    );

    queue.enqueue(task).unwrap();
    
    // Keep your app alive
    tokio::time::sleep(Duration::from_secs(10)).await;
}
```

---


### ⚙️ Advanced Task Configuration (v0.2.1)
To power complex AI workflows, tasks can now be configured with advanced orchestration parameters:

* **`auto_dedupe` (`bool`)**: If set to `true`, the daemon computes a cryptographic hash of the `task_type` and `task_data`. If an identical payload is currently sitting in the queue pending execution, this new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users!
* **`urgency_score` (`float`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a true Binary Max-Heap to continually float tasks with the highest urgency score to the very front of the execution line. Standard tasks default to `0.0`.
* **`rate_limit_group` (`string`)**: A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control.
* **`max_per_minute` (`int`)**: Used in conjunction with `rate_limit_group`. If the queue processes more tasks in this group than the allowed limit within a 60-second rolling window, further tasks in this group are temporarily paused. This natively prevents 429 "Too Many Requests" errors when bursting third-party APIs.
* **`webhook_url` (`string`)**: By providing a webhook URL, SnerdQueue will completely bypass your local Rust closures and dispatch the task payload via an HTTP POST request directly to the specified URL.

### 🌐 HTTP Webhooks (Serverless Execution)
You can configure a task to execute externally via an HTTP POST request. By setting a `webhook_url`, the internal background processor will skip any registered handlers (`queue.register_task_handler`) and directly invoke the HTTP endpoint.

If the HTTP endpoint returns a non-200 status code, it triggers a retry. If it permanently fails (reaches `max_retries`), the Dead Letter Queue event is automatically fired via a final HTTP POST to the same `webhook_url` but with the header `X-SnerdMQ-Event: MaxRetriesReached`.

## 🧠 Architecture Details

`snerd-rust` utilizes an **Append-Only Log Model** to achieve massive write speeds.
Instead of updating rows in a database, every time a task is enqueued, updated, or deleted, a brand new JSON line is instantly appended to the end of the log file.

When the `SnerdQueue` wakes up on its polling interval, it scans the log, maps out the absolute latest state of every task, and spawns parallel Tokio tasks for anything that is currently due (`retry_after_time <= now`). 

If your file ever grows too large (default `20MB` or >10k operations), `snerd-rust` atomically clones, shrinks, and replaces the file in the background (Log Compaction) to keep disk space minimal.

---

## 🤝 License

MIT License. Do whatever you want with it, just don't let your tasks die unhandled.
