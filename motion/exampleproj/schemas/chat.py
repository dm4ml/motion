import motion


class QuerySource(motion.MEnum):
    OFFLINE = "Offline"
    ONLINE = "Online"


class Chat(motion.Schema):
    src: QuerySource
    session_id: str
    prompt: str
    completion: str
    feedback: bool
