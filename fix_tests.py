import re

for filepath in ['tests/integration_test.rs', 'tests/rate_limit_test.rs']:
    with open(filepath, 'r') as f:
        content = f.read()

    # Find `RetryableTask::new(...)` and insert `None, None` at the end
    # Using regex to find the end of the new() block
    # It looks like:
    #     let task = RetryableTask::new(
    #         "task-1".to_string(),
    #         ...
    #         None,
    #     );
    
    # Let's just do a simple replacement for the lines ending with `None,` immediately before `    );`
    content = re.sub(r'None,\n\s+\);', r'None,\n        None,\n        None,\n    );', content)
    content = re.sub(r'None,\n\s+}\)', r'None,\n                None,\n                None,\n            })', content) # in case it's in a closure
    
    with open(filepath, 'w') as f:
        f.write(content)
