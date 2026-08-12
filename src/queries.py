"""Downstream query helpers that reproduce the reported symptom.

These mirror the SQL endpoints /sql/clinical/deviation-summary and
/sql/clinical/deviation-actions used by clinical operations.
"""
import json

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

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


def _read(prefix, client=None):
    client = client or _s3()
    key = f"{prefix}/data.json"
    obj = client.get_object(Bucket=cfg.LAKEHOUSE_BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def deviation_summary(study_id, client=None):
    """Return the count of deviations for a study from the gold fact.

    This intentionally remains a simple row count.  The grain fix belongs in
    silver/gold modeling: gold/fact_protocol_deviation must contain exactly one
    row per deviation.
    """
    fact = _read(cfg.GOLD_FACT_PREFIX, client)
    return sum(1 for r in fact if r.get("study_id") == study_id)


def _is_missing_key_error(exc):
    return exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}


def deviation_actions(deviation_id, client=None):
    """Return corrective actions for a given deviation.

    The preferred source is the silver action detail table, whose grain is one
    row per corrective action.  The legacy flattened table remains only as a
    fallback for compatibility with partially-built environments.
    """
    client = client or _s3()
    try:
        actions = _read(cfg.SILVER_ACTION_PREFIX, client)
        result = [a for a in actions if a.get("deviation_id") == deviation_id]
    except ClientError as exc:
        if not _is_missing_key_error(exc):
            raise
        flat = _read(cfg.SILVER_FLAT_PREFIX, client)
        result = [r for r in flat if r.get("deviation_id") == deviation_id and r.get("action_id")]

    return sorted(
        result,
        key=lambda r: (
            r.get("action_ordinal") if r.get("action_ordinal") is not None else 10**9,
            r.get("action_id") or "",
        ),
    )
