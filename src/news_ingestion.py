import boto3
from src.config import RAW_NEWS_PATH, S3_BUCKET, AWS_REGION
from src.logger import logger
import os

def upload_news_to_s3():
    if not os.path.exists(RAW_NEWS_PATH):
        raise FileNotFoundError("Local financial_news.csv not found")

    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.upload_file(
        RAW_NEWS_PATH,
        S3_BUCKET,
        "news/financial_news.csv"
    )

    logger.info("Uploaded financial_news.csv to S3")

if __name__ == "__main__":
    upload_news_to_s3()
