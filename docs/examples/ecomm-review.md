# Building an intelligent review aggregator

In this tutorial we'll use Motion to collect and analyze product reviews to give new customers a better idea of what others liked or didn't like about an item.

## Prerequisites
```bash
pip install motion-python
pip install openai
pip install textblob
pip install nltk
```

Ensure the `punkt` and `stopwords` NLTK packages are also installed by opening a new python interpreter and running `import nltk; nltk.download('punkt'); nltk.download('stopwords')` 

## Step 1
Import the libraries we'll be using; Motion for the backend, OpenAI as the LLM provider, and NLTK and Textblob for sentiment analysis. Most popular language models will work just fine. Also set your API key if needed.

```python
from motion import Component

import openai
import nltk
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textblob import TextBlob

from datetime import datetime, timedelta

openai.api_key = "sk-..."
```

## Step 2
Define a function we'll use to extract sentiment information from our review corpus. Think of this function as being similar to a cron job that runs asynchronously to parse data. *Note: Motion lets you decide under what conditions this function is executed. To force an update anytime `component.serve()` is called you can pass `component.run(flush_update=True)`.

```python
def analyze_review(review, recommended):
    blob = TextBlob(review)
    sentiment = blob.sentiment.polarity  # range is -1 to 1

    tokens = word_tokenize(review.lower())
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word not in stop_words and word not in string.punctuation]

    freq = nltk.FreqDist(filtered_tokens)
    total_words = sum(freq.values())

    keywords = []
    for word, count in freq.items():
        word_sentiment = TextBlob(word).sentiment.polarity
        significance = (count / total_words) * (word_sentiment * sentiment if sentiment != 0 else 0)
        keywords.append([word, abs(significance)])  # using abs to measure intensity disregarding polarity

    keywords.sort(key=lambda x: x[1], reverse=True)

    if keywords:
        max_significance = keywords[0][1]
        keywords = [[word, significance / max_significance] for word, significance in keywords]

    return sentiment, keywords[:5], recommended
```

This function is being used as a short example of what information you might want to extract about natural language data. First it analyzes the overall sentiment polarity of the review. Then it preprocesses the words in the review and calculates each word's significance with regard to the review's overall sentiment. Finally it outputs an overall sentiment score, as well as the top 5 keywords that contributed most to this sentiment.

Here's an example:
```python
review_text = """
This flashlight is the best I've ever owned. 
It's super bright and the battery lasts a long time. I love it!
"""
sentiment, keywords = analyze_review(review_text)
[['best', 1.0],
 ['bright', 0.7],
 ['love', 0.5],
 ['super', 0.3],
 ['long', 0.05]]
```

## Step 3
Next we initialize our review component in Motion. Here we're operating under the assumption that each product has it's own list of reviews, but you may just as well create a component for an entire storefront or a product category. Our intial data is also being populated with information that could have been loaded from disk.

```python
review = Component("Review")

@review.init_state
def setUp():
    return {
        "id": 0,
        "sentiment": [0.5],
        "keywords": [["heavy", "expensive", "bright"]],
        "would_recommend": [0]
    }
```

Our intial `Review` object for a flashlight contains a single review, where the general sentiment was neutral, used the keywords "heavy, expensive, and bright", and did not result in a recommendation.

# Step 4
TODO