# Test Connections

Oftentimes when developing a Motion application, you will want to test the application without serving it and tearing it down constantly. You can use the `motion.test` function to connect to a test server that is automatically started and torn down for you. `motion.test` returns a `ClientConnection` object.

### Session Scope

Every test connection is associated with a session. If a `session_id` is not specified, a new session is created. This is useful for testing the application from a clean slate. The `session_id` is a random string of characters, and each `ClientConnection` object has a `session_id` attribute that you can print out.

In serving mode (i.e., `motion serve`), the session is persistent, and the `session_id = "PRODUCTION"`.

::: motion.test
    handler: python
    options:
      heading_level: 3
      show_root_full_path: true
      show_root_toc_entry: true
      show_root_heading: true
      show_source: false

### Example Usage

Here is an example of how to use `motion.test` to create a test connection. Suppose we have a file called `test_single_chat.py` in our application directory with the following code:

```python title="test_single_chat.py"
from mconfig import MCONFIG
import motion

connection = motion.test(
    MCONFIG,
    wait_for_triggers=[], # No cron-scheduled triggers in our chatbot
    motion_logging_level="INFO"
)
print(f"Session ID: {connection.session_id}") # (1)!

# Must specify keywords for every arg in .set and .get
new_id = connection.set(
    relation="Query",
    identifier="",
    key_values={"prompt": "What color is the sky?"},
)
prompts_and_completions = connection.get(
    relation="Query",
    identifier=new_id,
    keys=["prompt", "llm_completion"],
    include_derived=True, # (2)!
    as_df=True,
)
print(f"Response: {prompts_and_completions}")

connection.checkpoint() # (3)!
```

1. The `session_id` will be a random string of characters. In another call to `motion.test`, we can set `session_id` to the same value to keep the same session.
2. We want to include the many LLM completions that were generated for a single prompt, so we set `include_derived=True`.
3. This will save the prompts and completions added in this session to disk, in case we want to reinitialize the session later. Otherwise, checkpointing is only done at the interval specified in the `MCONFIG`.