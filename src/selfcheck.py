"""Readiness self-check: init bucket, land bronze fixtures, verify connectivity.

Does NOT run candidate invariant tests and does NOT depend on the correctness
of the curated/gold model.
"""
import json
import sys

import boto3
from botocore.client import Config

from src import lakehouse_config as cfg


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=cfg.MINIO_ENDPOINT,
        aws_access_key_id=cfg.MINIO_ACCESS_KEY,
        aws_secret_access_key=cfg.MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client):
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if cfg.LAKEHOUSE_BUCKET not in existing:
        client.create_bucket(Bucket=cfg.LAKEHOUSE_BUCKET)


def land_bronze(client):
    with open(cfg.SOURCE_FIXTURE, "r", encoding="utf-8") as fh:
        records = json.load(fh)
    client.put_object(
        Bucket=cfg.LAKEHOUSE_BUCKET,
        Key=f"{cfg.BRONZE_PREFIX}/data.json",
        Body=json.dumps(records, sort_keys=True).encode("utf-8"),
    )
    return len(records)


def init():
    client = _s3()
    ensure_bucket(client)
    count = land_bronze(client)
    print(f"Initialized bucket '{cfg.LAKEHOUSE_BUCKET}' and landed {count} bronze deviation records.")
    return True


def verify():
    client = _s3()
    client.head_object(Bucket=cfg.LAKEHOUSE_BUCKET, Key=f"{cfg.BRONZE_PREFIX}/data.json")
    print("Bronze fixture present and object storage reachable.")
    return True


if __name__ == "__main__":
    if "--init" in sys.argv:
        init()
        verify()
    else:
        verify()
