from datetime import datetime

import motion


class QuerySource(motion.MEnum):
    OFFLINE = "Offline"
    ONLINE = "Online"


class Chat(motion.Schema):
    src: QuerySource
    session_id: str
    prompt: str
    full_prompt: str
    completion: str
    feedback: bool


class WikiEdit(motion.Schema):
    title: str
    pageid: str
    user: str
    userid: str
    comment: str
    edited_timestamp: datetime
