import re
from textblob import TextBlob


def remove_html_tags(text: str) -> str:
    if text is None:
        return None
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


def analyze_sentiment(text: str) -> str:
    if text is None:
        return None
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'
