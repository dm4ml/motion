import cohere
import motion
import os


class Chatbot(motion.Trigger):
    def routes(self):
        return [
            motion.Route(
                namespace="chat",
                key="prompt",
                infer=self.getCompletion,
                fit=None,
            )
        ]

    def setUp(self, cursor):
        # Set up the LLM
        return {"cohere": cohere.Client(os.environ["COHERE_API_KEY"])}

    def getCompletion(self, cursor, triggered_by):
        # Generate the completion from the LLM
        prompt = triggered_by.value
        response = self.state["cohere"].generate(
            prompt=prompt,
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
            triggered_by.namespace,  # "chat"
            identifier=triggered_by.identifier,
            key_values={"completion": completion},
        )
