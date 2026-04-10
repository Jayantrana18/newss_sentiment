from src.s3_loader import get_s3_client
from src.config import S3_BUCKET

s3 = get_s3_client()

# Just list first few objects
response = s3.list_objects_v2(Bucket=S3_BUCKET)

print("Objects in bucket:")
for obj in response.get("Contents", [])[:5]:
    print(" -", obj["Key"])
