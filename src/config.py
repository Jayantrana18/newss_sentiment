import os
from dotenv import load_dotenv

# Load environment variables from .env (if present)
load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# ========== AWS CONFIG ==========
# Prefer common AWS env var names, fall back to shorter names if used.
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_NEWS_KEY = "news/raw_partner_headlines.csv"
# ========== MODEL CONFIG ==========
MODEL_NAME = "ProsusAI/finbert"

# ========== DATA PATHS ==========
RAW_NEWS_PATH = os.path.join(BASE_DIR, "data", "raw_partner_headlines.csv")
SENTIMENT_OUTPUT_PATH = os.path.join(
    BASE_DIR, "data", "news_with_sentiment.csv"
)
STOCK_PRICE_PATH = os.path.join(
    BASE_DIR, "data", "reliance_prices.csv"
)