import cohere
import motion
import os
import re


class SuggestIdea(motion.Trigger):
    def setUp(self, cursor):
        # Set up the query suggestion model
        return {"cohere": cohere.Client(os.environ["COHERE_API_KEY"])}

    def shouldInfer(self, cursor, id, triggered_by):
        return True

    def infer(self, cursor, id, triggered_by):
        # Generate the query suggestions
        query = triggered_by.value
        prompt = (
            f"List 5 detailed outfit ideas for a woman to wear to {query}."
        )
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
        text = response[0].text
        suggestions = [s.strip() for s in text.split("\n")[:5]]
        suggestions = [re.sub("[1-9]. ", "", s) for s in suggestions]
        suggestions = [s for s in suggestions if s != ""]

        for s in suggestions:
            new_id = cursor.duplicate("query", id=id)
            cursor.set("query", id=new_id, key_values={"text_suggestion": s})

    def shouldFit(self, cursor, id, triggered_by):
        # Check if fit should be called
        return False

    def fit(self, cursor, id, triggered_by):
        # Fine-tune or fit the query suggestion model
        pass
