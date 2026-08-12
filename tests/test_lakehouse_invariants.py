"""Candidate-facing invariant tests for the protocol deviation data product.

These verify business outcomes:
  - gold deviation summary matches source distinct deviation counts
  - action drill-through preserves all corrective actions
  - the gold fact is at deviation grain (one row per deviation_id)
  - pipeline reruns are idempotent
  - required-field data-quality expectations hold on curated deviations

They may FAIL on the unsolved starter and PASS after a correct fix.
"""
import json

import boto3
import pytest
from botocore.client import Config

from src import lakehouse_config as cfg
from src import pipeline, queries, selfcheck


def _client():
    return boto3.client(
        "s3",
        endpoint_url=cfg.MINIO_ENDPOINT,
        aws_access_key_id=cfg.MINIO_ACCESS_KEY,
        aws_secret_access_key=cfg.MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _read(client, prefix):
    key = f"{prefix}/data.json"
    obj = client.get_object(Bucket=cfg.LAKEHOUSE_BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


@pytest.fixture(scope="module", autouse=True)
def built_pipeline():
    client = _client()
    selfcheck.ensure_bucket(client)
    selfcheck.land_bronze(client)
    pipeline.run_pipeline(client)
    return client


def _source_distinct_count(study_id):
    with open(cfg.SOURCE_FIXTURE, "r", encoding="utf-8") as fh:
        records = json.load(fh)
    return len({r["deviation_id"] for r in records if r["study_id"] == study_id})


def test_deviation_summary_matches_source_distinct_count():
    expected = _source_distinct_count("IMM-07")
    actual = queries.deviation_summary("IMM-07")
    assert actual == expected, (
        f"deviation-summary for IMM-07 returned {actual}, expected {expected} distinct deviations"
    )


def test_gold_fact_is_deviation_grain():
    client = _client()
    fact = _read(client, cfg.GOLD_FACT_PREFIX)
    ids = [r["deviation_id"] for r in fact]
    assert len(ids) == len(set(ids)), "gold fact contains duplicate deviation_id rows; grain is not deviation-level"


def test_action_drill_through_preserves_all_actions():
    actions = queries.deviation_actions("PD-8831")
    action_ids = {a.get("action_id") for a in actions}
    assert {"CA-1", "CA-2", "CA-3"}.issubset(action_ids), (
        f"expected all three corrective actions for PD-8831, got {action_ids}"
    )


def test_pipeline_rerun_is_idempotent():
    client = _client()
    pipeline.run_pipeline(client)
    first_fact = _read(client, cfg.GOLD_FACT_PREFIX)
    pipeline.run_pipeline(client)
    second_fact = _read(client, cfg.GOLD_FACT_PREFIX)
    assert len(first_fact) == len(second_fact), "gold fact row count changed on rerun; pipeline is not idempotent"


def test_required_fields_enforced_on_curated_deviations():
    from src import models
    bad = {"deviation_id": None, "study_id": "IMM-07", "deviation_date": "2024-02-11"}
    good = {"deviation_id": "PD-8831", "study_id": "IMM-07", "deviation_date": "2024-02-11"}
    assert models.deviation_required_ok(bad) is False
    assert models.deviation_required_ok(good) is True


def test_deviation_with_no_actions_still_counts_once():
    summary = queries.deviation_summary("IMM-07")
    client = _client()
    fact = _read(client, cfg.GOLD_FACT_PREFIX)
    fact_ids = {r["deviation_id"] for r in fact}
    assert "PD-8834" in fact_ids, "deviation with zero corrective actions must still appear once in the gold fact"
