# prompts.py

NER_PROMPT = """
Perform Named Entity Recognition on the following text. Identify and classify named entities such as person names, organizations, locations, medical terms, etc.

Text: {text}

Please return the results in the following format:
Entity: Type

"""

SENTIMENT_PROMPT = """
Perform sentiment analysis on the following text. Determine whether the sentiment is positive, negative, or neutral.

Text: {text}

Please return the results in the following format:
Sentiment: [Positive/Negative/Neutral]
Confidence: [0-1 scale]
Explanation: [Brief explanation of the sentiment]

"""

ZERO_SHOT_PROMPT = """
Perform zero-shot classification on the following text for the given categories. Determine which category the text belongs to without any prior training examples.

Text: {text}

Categories: {categories}

Please return the results in the following format:
Category: [Most likely category]
Confidence: [0-1 scale]
Explanation: [Brief explanation of the classification]

"""

RAG_PROMPT = """
You are a helpful assistant that answers questions based on the given context. Maintain continuity in the conversation and refer to previous messages when necessary.

Context: {context}

Question: {question}

Please provide a concise and accurate answer based on the given context.

"""
