use snerd_rust::file_store::FileStore;
use snerd_rust::queue::SnerdQueue;
use snerd_rust::rate_limiter::RateLimiter;

#[test]
#[should_panic(expected = "Another queue instance is already running")]
fn second_queue_on_same_storage_panics() {
    let temp_dir = tempfile::tempdir().unwrap();
    let log = temp_dir.path().join("tasks.log");

    let store1 = FileStore::new(&log).unwrap();
    let _q1 = SnerdQueue::new("first", store1, RateLimiter::new(&log));

    // Same storage file while the first queue is alive must fail fast
    let store2 = FileStore::new(&log).unwrap();
    let _q2 = SnerdQueue::new("second", store2, RateLimiter::new(&log));
}

#[test]
fn lock_released_when_queue_dropped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let log = temp_dir.path().join("tasks.log");

    let store1 = FileStore::new(&log).unwrap();
    let q1 = SnerdQueue::new("first", store1, RateLimiter::new(&log));
    drop(q1);

    // After the first queue is gone, the storage is free again
    let store2 = FileStore::new(&log).unwrap();
    let _q2 = SnerdQueue::new("second", store2, RateLimiter::new(&log));
}
