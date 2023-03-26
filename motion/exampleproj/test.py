from __future__ import annotations

from mconfig import MCONFIG
from rich import print

import motion

# Test that for simple queries, the results make some sense


def test_ask_chatbot():
    connection = motion.test(
        MCONFIG,
        wait_for_triggers=[],  # No triggers to wait for
        motion_logging_level="INFO",
    )

    all_prompts = [
        "What should I wear to a wedding?",
        "What should I wear to a party?",
        "What should I wear to a job interview?",
        "What should I wear to a first date?",
        "What should I wear to a picnic?",
    ]

    for prompt in all_prompts:
        # Must specify kw for every arg in .set and .get
        new_id = connection.set(
            relation="chat",
            key_values={"prompt": prompt},
        )
        result = connection.get(relation="chat", identifier=new_id, keys=["completion"])
        print(f"Prompt: {prompt} and result: {result}")

    connection.close(wait=False)


test_ask_chatbot()
