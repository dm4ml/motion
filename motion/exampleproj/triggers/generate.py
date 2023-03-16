import random


def sample_prompt_generator(cursor):
    # Emit sample prompts to the DB
    all_prompts = [
        "What should I wear to a wedding?",
        "What should I wear to a party?",
        "What should I wear to a job interview?",
        "What should I wear to a first date?",
        "What should I wear to a picnic?",
    ]

    rand_idx = random.randint(0, len(all_prompts) - 1)

    new_id = cursor.getNewId("catalog")
    cursor.set(
        "chat", identifier=new_id, key_values={"prompt": all_prompts[rand_idx]}
    )
