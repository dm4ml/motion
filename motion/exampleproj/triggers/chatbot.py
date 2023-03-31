import os
from collections import deque

import cohere

import motion


class Chatbot(motion.Trigger):
    def routes(self):
        return [
            motion.Route(
                relation="Chat",
                key="prompt",
                infer=self.getCompletion,
                fit=None,
            ),
            motion.Route(
                relation="WikiEdit",
                key="title",
                infer=None,
                fit=self.addToPrompt,
            ),
        ]

    def setUp(self, cursor):
        # Set up the LLM
        return {
            "cohere": cohere.Client(os.environ["COHERE_API_KEY"]),
            "recent_titles": deque(maxlen=10),
        }

    def getCompletion(self, cursor, triggered_by):
        # Generate the completion from the LLM
        prompt = triggered_by.value
        concatenated_recent_titles = ", ".join(list(self.state["recent_titles"]))

        full_prompt = (
            prompt
            + "\n"
            + f"The last ten Wikipedia pages edited were: {concatenated_recent_titles}"
        )

        response = self.state["cohere"].generate(
            prompt=full_prompt,
            model="command-xlarge-nightly",
            max_tokens=300,
            temperature=0.9,
            k=0,
            p=0.75,
            stop_sequences=[],
            return_likelihoods="NONE",
        )
        completion = response[0].text
        cursor.set(
            relation=triggered_by.relation,  # "chat"
            identifier=triggered_by.identifier,
            key_values={"full_prompt": full_prompt, "completion": completion},
        )

    def addToPrompt(self, cursor, triggered_by):
        # Add the title of the recently edited Wikipedia page to the prompt

        recent_titles = self.state["recent_titles"]
        recent_titles.append(triggered_by.value)
        return {"recent_titles": recent_titles}
