import os

import boto3
from loguru import logger

def get_minio_client():

    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    aws_access_key = os.getenv("MINIO_USER")
    aws_secret_access_key = os.getenv("MINIO_PASSWORD")
    bucket = os.getenv("MINIO_BUCKET")

    if not minio_endpoint:
        raise ValueError("MINIO_ENDPOINT is not configured")

    if not aws_access_key:
        raise ValueError("MINIO_ACCESS_KEY is not configured")
    
    if not aws_secret_access_key:
        raise ValueError("MINIO_SECRET_KEY is not configured")

    logger.info("Opening connection to S3")

    client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
    )
    return client, bucket