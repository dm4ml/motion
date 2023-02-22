"""This is a sample UI for the Journal Copilot. It is a simple
pipeline to autocomplete a journal entry.
"""

from dataclasses import dataclass
from enum import Enum
from langchain import HuggingFaceHub
from langchain.text_splitter import MarkdownTextSplitter
from langchain import PromptTemplate, FewShotPromptTemplate
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import FAISS
from langchain.docstore.document import Document
from langchain.chains import LLMChain

import motion
import sqlite3
import pandas as pd

con = sqlite3.connect(
    "/Users/shreyashankar/Library/Group Containers/9K33E3U3T4.net.shinyfrog.bear/Application Data/database.sqlite"
)
cur = con.cursor()
note_ids = cur.execute(
    "SELECT Z_PK FROM ZSFNOTE WHERE ZTRASHED != 1"
).fetchall()
note_id_str = ", ".join([str(note_id[0]) for note_id in note_ids])

df = pd.read_sql_query(
    f"SELECT * FROM ZSFNOTE WHERE Z_PK IN ({note_id_str})", con
)
con.close()


# Step 1: Define the store schema


class DataSource(Enum):
    JOURNAL = 0
    ONLINE = 1


@dataclass
class JournalSchema:
    id: int
    ts: int
    src: DataSource
    raw: str
    prompt: str
    completion: str
    feedback: str
    prediction: str
    page: int = 1


# Step 2: Define the pipeline components. We grab semantically similar prompts and embed the prompt-completion pairs in the prompts.


def gen_prompt_completion(store, id):
    text_splitter = MarkdownTextSplitter(
        chunk_size=150,
        chunk_overlap=0,
        length_function=len,
    )
    src, raw, feedback, page = store.mget(
        id, ["src", "raw", "feedback", "page"]
    )
    if src == DataSource.JOURNAL or feedback:
        texts = text_splitter.create_documents([raw])
        texts = [text.page_content for text in texts]
        for i, (prompt, completion) in enumerate(zip(texts[:-1], texts[1:])):
            store.mset(
                id,
                {"prompt": prompt, "completion": completion, "page": page + i},
            )
    else:
        prompt = raw
        completion = None
        store.mset(id, {"prompt": prompt, "completion": completion})


class Model(motion.Transform):
    def setUp(self):
        # Set up prompt template
        formatter_string = """
        Text: {prompt}
        Completion: {completion}\n
        """
        self.formatter_template = PromptTemplate(
            input_variables=["prompt", "completion"],
            template=formatter_string,
        )
        self.hf_embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-mpnet-base-v2"
        )
        self.top_k = 3
        self.llm = HuggingFaceHub(
            repo_id="google/flan-t5-xl",
            model_kwargs={"temperature": 0.9, "max_length": 64},
        )

    def fit(self, ids, iter):
        prompts, completions = self.store.sql(
            f"SELECT prompt, completion FROM journal WHERE completion IS NOT NULL AND id IN ({ids})",
        )
        # Create embeddings for prompt-completion pairs
        prompt_docs = [
            Document(page_content=prompt, metadata={"completion": completion})
            for prompt, completion in zip(prompts, completions)
        ]
        self.db = FAISS.from_documents(prompt_docs, self.hf_embeddings)

    def __call__(self, id):
        prompt, src = self.store.mget(id, ["prompt", "src"])

        # Don't do anything if the prompt is from the journal
        if src == DataSource.JOURNAL:
            return

        docs_and_scores = self.db.similarity_search_with_score(
            prompt, k=self.top_k
        )
        examples = [
            {"prompt": d.page_content, "completion": d.metadata["completion"]}
            for d, _ in docs_and_scores
        ]

        # Format prompt
        few_shot_prompt = FewShotPromptTemplate(
            examples=examples,
            example_prompt=self.formatter_template,
            prefix="Generate a completion for a journal entry. Use the following examples as a guide.",
            suffix="Text: {input}\nCompletion:",
            input_variables=["input"],
            example_separator="\n\n",
        )
        chain = LLMChain(llm=self.llm, prompt=few_shot_prompt)
        prediction = chain.run(input=prompt)
        self.store.set(id, "prediction", prediction)


# Step 3: Create a store and attach the pipeline components as triggers

store = motion.create_or_get_store(name="journal", schema=JournalSchema)
store.createTrigger(keys=["chunk"], executable=gen_prompt_completion)
store.createTrigger(keys=["prompt"], executable=Model)


### Scratch

# llm = HuggingFaceHub(
#     repo_id="google/flan-t5-xl",
#     model_kwargs={"temperature": 0.9, "max_length": 64},
# )
# text_splitter = MarkdownTextSplitter(
#     chunk_size=150,
#     chunk_overlap=0,
#     length_function=len,
# )
# texts = text_splitter.create_documents(df["ZTEXT"].values)

# hf_embeddings = HuggingFaceEmbeddings(
#     model_name="sentence-transformers/all-mpnet-base-v2"
# )
# db = FAISS.from_documents(texts, hf_embeddings)
# docs_and_scores = db.similarity_search_with_score("How is my PhD going?", k=15)
# print(docs_and_scores)
# db.save_local("faiss_index")
