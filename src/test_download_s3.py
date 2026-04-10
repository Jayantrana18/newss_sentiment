import os
from src.s3_loader import download_from_s3
from src.config import RAW_NEWS_PATH, S3_NEWS_KEY

# Delete local file if it exists
if os.path.exists(RAW_NEWS_PATH):
    os.remove(RAW_NEWS_PATH)
    print("Deleted local file")

# Download from S3
download_from_s3(
    s3_key=S3_NEWS_KEY,
    local_path=RAW_NEWS_PATH
)

print("Downloaded file exists:", os.path.exists(RAW_NEWS_PATH))
