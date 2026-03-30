import os
import boto3
from botocore.client import Config

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "files-bucket")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    config=Config(signature_version="s3v4"),
)

def _ensure_bucket():
    names = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if S3_BUCKET not in names:
        s3.create_bucket(Bucket=S3_BUCKET)

_ensure_bucket()

def s3_upload_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=content_type)

def s3_download_bytes(key: str) -> bytes:
    return s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()

def s3_delete(key: str) -> None:
    s3.delete_object(Bucket=S3_BUCKET, Key=key)