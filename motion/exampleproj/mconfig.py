from schemas import Chat
from triggers import Chatbot, sample_prompt_generator

MCONFIG = {
    "application": {"name": "{0}", "author": "{1}", "version": "0.1"},
    "namespaces": {"chat": Chat},
    "triggers": {
        Chatbot: ["chat.prompt"],
        sample_prompt_generator: ["*/1 * * * *"],
    },
    "checkpoint": "0 * * * *",
}
