with open('tests/integration_test.rs', 'r') as f:
    content = f.read()

# Replace missing imports by adding them at the top
imports = """
use std::sync::Mutex;
use chrono::Utc;
use snerd_rust::rate_limiter::RateLimiter;
use tempfile::tempdir;
"""
if "use std::sync::Mutex;" not in content:
    content = imports + content

# Fix the test handler
old_handler = """        .register_task_handler("cron-task", move |_| {
            let e = exec_clone.clone();
            async move {
                *e.lock().unwrap() = true;
                Ok(())
            }
        })"""
new_handler = """        .register_task_handler("cron-task", move |_data| {
            let e = exec_clone.clone();
            *e.lock().unwrap() = true;
            Ok(())
        })"""

content = content.replace(old_handler, new_handler)

with open('tests/integration_test.rs', 'w') as f:
    f.write(content)
