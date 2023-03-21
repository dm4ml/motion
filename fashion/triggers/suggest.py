import cohere
import motion
import os
import re


class SuggestIdea(motion.Trigger):
    def routes(self):
        return [
            motion.Route(
                namespace="query",
                key="query",
                infer=self.generateSuggestions,
                fit=None,
            )
        ]

    def setUp(self, cursor):
        # Set up the query suggestion model
        return {"cohere": cohere.Client(os.environ["COHERE_API_KEY"])}

    def generateSuggestions(self, cursor, triggered_by):
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
            new_id = cursor.duplicate(
                triggered_by.namespace, identifier=triggered_by.identifier
            )
            cursor.set(
                triggered_by.namespace,
                identifier=new_id,
                key_values={"text_suggestion": s},
            )
