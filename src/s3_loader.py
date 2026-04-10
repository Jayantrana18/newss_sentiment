import boto3
import os
import sys
from src.config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_BUCKET
from src.logger import logger

def get_s3_client():
    """Return a boto3 S3 client. If credentials are provided in config, use them;
    otherwise rely on the environment / IAM role / shared credentials file.
    """
    client_kwargs = {}
    if AWS_REGION:
        client_kwargs["region_name"] = AWS_REGION

    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        client_kwargs["aws_access_key_id"] = AWS_ACCESS_KEY
        client_kwargs["aws_secret_access_key"] = AWS_SECRET_KEY

    try:
        return boto3.client("s3", **client_kwargs)
    except Exception as e:
        logger.error("Failed creating S3 client: %s", e)
        raise


def download_from_s3(s3_key, local_path):
    try:
        if not S3_BUCKET:
            logger.error("S3_BUCKET is not set. Please set S3_BUCKET in your environment or .env file.")
            raise ValueError("S3_BUCKET not configured")

        s3 = get_s3_client()
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        logger.info(f"Downloading {s3_key} from S3 bucket {S3_BUCKET}...")
        s3.download_file(S3_BUCKET, s3_key, local_path)

        logger.info(f"File saved to {local_path}")

    except Exception as e:
        logger.error(f"Failed to download {s3_key} : {e}")
        raise


if __name__ == "__main__":
    # Example downloads
    try:
        download_from_s3("news/financial_news.csv", "data/financial_news.csv")
        download_from_s3("stocks/indian_prices.csv", "data/indian_prices.csv")
    except Exception:
        sys.exit(1)
