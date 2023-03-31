from mconfig import MCONFIG
from rich import print

import motion

# Test that for simple queries, the results make some sense


def test_ask_chatbot():
    connection = motion.test(
        MCONFIG,
        wait_for_triggers=["ScrapeWikipedia"],
        motion_logging_level="WARNING",  # Can be "INFO" or "DEBUG" for more verbose logging
    )

    # Must specify kw for every arg in .set and .get
    prompt = "What do people find interesting?"
    new_id = connection.set(
        relation="Chat",
        identifier="",
        key_values={"prompt": prompt},
    )
    result = connection.get(
        relation="Chat", identifier=new_id, keys=["completion", "full_prompt"]
    )
    print(f"Prompt: {prompt}")
    print(f"Full prompt: {result['full_prompt']}")
    print(f"Response: {result['completion']}")

    connection.close(wait=False)


test_ask_chatbot()
