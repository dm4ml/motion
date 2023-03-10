import cohere
import motion
import os


class Chatbot(motion.Trigger):
    def setUp(self, cursor):
        # Set up the LLM
        return {"cohere": cohere.Client(os.environ["COHERE_API_KEY"])}

    def shouldInfer(self, cursor, id, triggered_by):
        return True

    def infer(self, cursor, id, triggered_by):
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
            id=id,
            key_values={"completion": completion},
        )

    def shouldFit(self, cursor, id, triggered_by):
        # Check if fit should be called
        return False

    def fit(self, cursor, id, triggered_by):
        # Fine-tune or fit the model
        pass
