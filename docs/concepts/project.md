# Project Structure

Whew, now the conceptual hurdles are out of the way. But how do we create an application in Motion? Let's start by looking at the project structure.

## Initializing a New Application

To create a new Motion application, run the following command:

```bash
motion create
```

You will be prompted to enter an application name and an author name. This will create a new directory with the name of your application in your current working directory. For example, if you enter `examplechatbot` as the application name, a new directory called `examplechatbot` will be created in your current working directory.

```bash
$ motion create
Your application name: examplechatbot
Your name: shreyashankar
Created a project successfully.
```

## Application Directory Structure

If you navigate into your application directory, for example:

```bash
cd examplechatbot
```

You will see the following directory structure:

    examplechatbot/
    ├── schemas/
    │   └── __init__.py
    ├── triggers/
    │   └── __init__.py
    └── mconfig.py

Put your schema definitions in the `schemas` directory. Put your trigger definitions in the `triggers` directory. For example, we can create a file called `chat.py` in the `schemas` directory and define a schema for chats:

```py title="schemas/chat.py"
import motion

class Query(motion.Schema):
    username: str
    prompt: str
    llm_completion: str
    llm_completion_score: float
    user_feedback: bool
```

And we can define a trigger for the chatbot:

```py title="triggers/chatbot.py"
import motion

class Chatbot(motion.Trigger):
    ... # (1)!

```

1. The `...` is a placeholder for the trigger definition.


## Configuring your Application

The `mconfig.py` file is where you will define your application's relations, triggers, and other metadata. The default `mconfig.py` file looks like this:

```py title="mconfig.py"
MCONFIG = {
    "application": {
        "name": "examplechatbot",
        "author": "shreyashankar",
        "version": "0.1",
    },
    "relations": [],
    "triggers": [],
    "trigger_params": {},
    "checkpoint": "0 * * * *",
}
```

### Configuring Schemas and Triggers

To update it with the application's relations and triggers, we edit the `relations` and `triggers` keys in the `MCONFIG` dictionary:

```python title="mconfig.py" hl_lines="1 2 10 11 12 13 14 15"
from schemas.chat import Query
from triggers.chatbot import Chatbot

MCONFIG = {
    "application": {
        "name": "examplechatbot",
        "author": "shreyashankar",
        "version": "0.1",
    },
    "relations": [
        Query,
    ],
    "triggers": [
        Chatbot,
    ],
    "trigger_params": {},
    "checkpoint": "0 * * * *",
}

```

### Configuring Checkpointing

The `checkpoint` key is used to define the frequency at which the data store will be checkpointed to disk. The default value is `0 * * * *`, which means the application will be executed at minute 0 (every hour). You can change this value to any valid cron expression. Checkpointing is done in the background in Motion.

### Configuring Trigger Parameters

The `trigger_params` key is used to define parameters that can be passed to triggers. This enables better model experimentation practices within Motion. For example, we can define a parameter called `index_interval` that can be passed to the `Chatbot` trigger to define the interval at which the chatbot's index should update. 

We do this by adding a dictionary to the `trigger_params` key in the `MCONFIG`:

```python
"trigger_params": {Chatbot: {"index_interval": 10}}
```

We can then access this parameter in the `Chatbot` trigger with the `self.params` dictionary:

```python hl_lines="17"
class Chatbot(motion.Trigger):

    def setUp(self, cursor):
        ... # Same as Trigger docs

        return {"index": index, "model": llm, "counter": 0}

    ... # Same routes, llm_infer as Trigger docs

    def update_index(self, cursor, triggered_by):
        ... # Same as trigger docs

        # Increment the counter
        counter = self.state["counter"] + 1

        # Update the index every `index_interval` prompts
        if counter % self.params["index_interval"] == 0: # (1)!
            new_index = update_index(self.state["index"], prompts_and_completions)
            return {"index": new_index, "counter": counter}

        return {"counter": counter}

```

1. In the [Trigger docs](/concepts/trigger/#how-do-i-maintain-a-counter-in-the-trigger-state), we hard-coded the `index_interval` parameter as 10.

The format for `trigger_params` is a dictionary mapping triggers to a dictionary of parameters. Parameters have string keys and can have any JSON-serializable value.