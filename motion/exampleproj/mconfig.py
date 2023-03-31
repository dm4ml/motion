from schemas import Chat, WikiEdit
from triggers import Chatbot, ScrapeWikipedia

MCONFIG = {
    "application": {"name": "{0}", "author": "{1}", "version": "0.1"},
    "relations": [Chat, WikiEdit],
    "triggers": [Chatbot, ScrapeWikipedia],
    "checkpoint": "0 * * * *",
}
