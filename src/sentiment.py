import os
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from src.s3_loader import download_from_s3
from src.config import (
    RAW_NEWS_PATH,
    SENTIMENT_OUTPUT_PATH,
    S3_NEWS_KEY,
    MODEL_NAME
)
from src.logger import logger


# -------------------------------
# Load FinBERT (CPU only)
# -------------------------------
logger.info("Loading FinBERT model...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
model.eval()


def ensure_news_data():
    """Ensure news data exists locally, otherwise download from S3."""
    if not os.path.exists(RAW_NEWS_PATH):
        logger.info("Local news file not found. Downloading from S3...")
        download_from_s3(
            s3_key=S3_NEWS_KEY,
            local_path=RAW_NEWS_PATH
        )
        logger.info("News file downloaded from S3.")
    else:
        logger.info("Local news file already exists.")


def get_sentiment(text):
    """Return sentiment label and confidence score."""
    inputs = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        padding=True,
        max_length=128
    )

    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.softmax(outputs.logits, dim=1)[0]

    labels = ["negative", "neutral", "positive"]
    idx = torch.argmax(probs).item()

    return labels[idx], float(probs[idx])


def run_sentiment_pipeline():
    ensure_news_data()

    logger.info("Reading news data...")
    df = pd.read_csv(RAW_NEWS_PATH, nrows=5000)

# Optional: confirm
    logger.info(f"Rows after sampling: {len(df)}")


    # Use headline column for sentiment
    headlines = df["headline"].astype(str).tolist()

    logger.info(f"Processing {len(headlines)} news headlines")

    sentiments = []
    scores = []

    for text in headlines:
        label, score = get_sentiment(text)
        sentiments.append(label)
        scores.append(score)

    df["sentiment"] = sentiments
    df["sentiment_score"] = scores

    df.to_csv(SENTIMENT_OUTPUT_PATH, index=False)
    logger.info(f"Sentiment data saved to {SENTIMENT_OUTPUT_PATH}")


if __name__ == "__main__":
    run_sentiment_pipeline()
