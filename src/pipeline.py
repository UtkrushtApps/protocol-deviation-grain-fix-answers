"""Protocol deviation medallion pipeline (bronze -> silver -> gold).

This is a local, Databricks-style simulation. Bronze fixtures land in MinIO;
silver and gold are materialized as in-memory table representations that are
persisted back to object storage so reruns are deterministic.

The important modeling rule is that protocol deviations and corrective actions
have different grains:

* silver/protocol_deviation: one row per distinct deviation_id
* silver/protocol_deviation_action: one row per corrective action
* gold/fact_protocol_deviation: one row per distinct deviation_id

The legacy flattened silver table is still written for compatibility, but gold
is intentionally built from the deviation-grain silver table, not from the
action-exploded table.
"""
import hashlib
import json
from datetime import timezone

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from src import lakehouse_config as cfg
from src import models


BRONZE_DATA_KEY = f"{cfg.BRONZE_PREFIX}/data.json"


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=cfg.MINIO_ENDPOINT,
        aws_access_key_id=cfg.MINIO_ACCESS_KEY,
        aws_secret_access_key=cfg.MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _read_json(client, prefix):
    key = f"{prefix}/data.json"
    obj = client.get_object(Bucket=cfg.LAKEHOUSE_BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def _write_json(client, prefix, rows):
    """Replace a materialized table atomically at the JSON-object level.

    The local scaffold stores each table as a single object.  Replacing the
    object instead of appending gives deterministic, idempotent reruns, mirroring
    an overwrite/merge pattern in a Delta Lake pipeline.
    """
    key = f"{prefix}/data.json"
    client.put_object(
        Bucket=cfg.LAKEHOUSE_BUCKET,
        Key=key,
        Body=json.dumps(rows, sort_keys=True).encode("utf-8"),
    )


def _record_hash(row, fields):
    payload = "|".join(str(row.get(f, "")) for f in fields)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _json_hash(value):
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _bronze_ingestion_ts(client):
    """Use the bronze object's LastModified time as stable ingestion lineage.

    This timestamp is stable across pipeline reruns as long as the bronze object
    is unchanged, so output rows remain idempotent while still carrying an audit
    timestamp tied to the source landing event.
    """
    obj = client.head_object(Bucket=cfg.LAKEHOUSE_BUCKET, Key=BRONZE_DATA_KEY)
    last_modified = obj["LastModified"]
    if last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=timezone.utc)
    return last_modified.astimezone(timezone.utc).isoformat()


def _sort_deviations(rows):
    return sorted(rows, key=lambda r: (r.get("study_id") or "", r.get("deviation_id") or ""))


def _sort_actions(rows):
    return sorted(
        rows,
        key=lambda r: (
            r.get("deviation_id") or "",
            r.get("action_ordinal") if r.get("action_ordinal") is not None else 10**9,
            r.get("action_id") or "",
        ),
    )


def build_bronze(client=None):
    """Bronze is landed by selfcheck --init; this returns the raw records."""
    client = client or _s3()
    return _read_json(client, cfg.BRONZE_PREFIX)


def build_silver(client=None):
    """Standardize bronze EDC records into curated silver tables.

    The starter implementation exploded corrective_actions[] and treated the
    flattened result as the only curated table.  That made the downstream gold
    fact action-grain and caused deviation counts to overcount deviations with
    multiple actions.

    This implementation separates the two grains:
      * deviation rows are validated and deduplicated by deviation_id
      * action rows are produced only for real corrective actions
      * actions are linked back to validated parent deviations
      * all rows carry source-system, stable ingestion timestamp, record hash,
        bronze object key, and bronze record hash lineage
    """
    client = client or _s3()
    bronze = build_bronze(client)
    ingestion_ts = _bronze_ingestion_ts(client)

    deviations_by_id = {}
    actions_by_key = {}

    for rec in bronze:
        bronze_record_hash = _json_hash(rec)
        lineage = {
            "source_system": cfg.SOURCE_SYSTEM,
            "ingestion_ts": ingestion_ts,
            "bronze_object_key": BRONZE_DATA_KEY,
            "bronze_record_hash": bronze_record_hash,
        }

        deviation = {
            "deviation_id": rec.get("deviation_id"),
            "study_id": rec.get("study_id"),
            "subject_id": rec.get("subject_id"),
            "site_id": rec.get("site_id"),
            "deviation_date": rec.get("deviation_date"),
            "deviation_category": rec.get("deviation_category"),
            "severity": rec.get("severity"),
            **lineage,
        }
        deviation["record_hash"] = _record_hash(
            deviation,
            [
                "deviation_id",
                "study_id",
                "subject_id",
                "site_id",
                "deviation_date",
                "deviation_category",
                "severity",
                "source_system",
                "bronze_record_hash",
            ],
        )

        # Enforce required fields before a record can enter curated silver or
        # contribute child action rows.  Invalid parent deviations are excluded
        # instead of creating orphaned facts/actions.
        if not models.deviation_required_ok(deviation):
            continue

        deviation_id = deviation["deviation_id"]
        deviations_by_id[deviation_id] = deviation

        corrective_actions = rec.get("corrective_actions") or []
        for ordinal, action in enumerate(corrective_actions, start=1):
            if not isinstance(action, dict):
                continue
            action_row = {
                "action_id": action.get("action_id"),
                "deviation_id": deviation_id,
                "action_ordinal": ordinal,
                "action_description": action.get("action_description"),
                "action_status": action.get("action_status"),
                **lineage,
            }
            action_row["record_hash"] = _record_hash(
                action_row,
                [
                    "deviation_id",
                    "action_id",
                    "action_ordinal",
                    "action_description",
                    "action_status",
                    "source_system",
                    "bronze_record_hash",
                ],
            )

            # A real action must have an action_id.  Empty action arrays create
            # no rows.  The action grain key includes parent deviation_id to
            # avoid collisions if source action identifiers are study-local.
            if not models.action_required_ok(action_row):
                continue
            actions_by_key[(deviation_id, action_row["action_id"])] = action_row

    deviation_rows = _sort_deviations(deviations_by_id.values())
    action_rows = _sort_actions(actions_by_key.values())

    _write_json(client, cfg.SILVER_DEVIATION_PREFIX, deviation_rows)
    _write_json(client, cfg.SILVER_ACTION_PREFIX, action_rows)

    # Compatibility materialization for legacy consumers.  This table is no
    # longer a source for the gold deviation fact.  Deviations with no actions
    # still get one flat row with null action columns for backward-compatible
    # inspection, while action drill-through reads the true action-grain table.
    actions_by_deviation = {}
    for action in action_rows:
        actions_by_deviation.setdefault(action["deviation_id"], []).append(action)

    flat_rows = []
    for deviation in deviation_rows:
        related_actions = actions_by_deviation.get(deviation["deviation_id"]) or [None]
        for action in related_actions:
            flat = dict(deviation)
            if action is None:
                flat.update(
                    {
                        "action_id": None,
                        "action_ordinal": None,
                        "action_description": None,
                        "action_status": None,
                    }
                )
            else:
                flat.update(
                    {
                        "action_id": action.get("action_id"),
                        "action_ordinal": action.get("action_ordinal"),
                        "action_description": action.get("action_description"),
                        "action_status": action.get("action_status"),
                    }
                )
            flat_rows.append(flat)
    _write_json(client, cfg.SILVER_FLAT_PREFIX, _sort_actions(flat_rows))

    return {"deviations": deviation_rows, "actions": action_rows, "flat": flat_rows}


def _read_or_build_silver_deviations(client):
    try:
        return _read_json(client, cfg.SILVER_DEVIATION_PREFIX)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code not in {"NoSuchKey", "404", "NotFound"}:
            raise
        return build_silver(client)["deviations"]


def build_gold(client=None):
    """Build the deviation fact from the deviation-grain curated silver layer."""
    client = client or _s3()
    silver_deviations = _read_or_build_silver_deviations(client)

    fact_rows = []
    for r in silver_deviations:
        if not models.deviation_required_ok(r):
            continue
        fact_rows.append(
            {
                "deviation_id": r.get("deviation_id"),
                "study_id": r.get("study_id"),
                "subject_id": r.get("subject_id"),
                "site_id": r.get("site_id"),
                "deviation_date": r.get("deviation_date"),
                "deviation_category": r.get("deviation_category"),
                "severity": r.get("severity"),
                "source_system": r.get("source_system"),
                "ingestion_ts": r.get("ingestion_ts"),
                "record_hash": r.get("record_hash"),
                "bronze_object_key": r.get("bronze_object_key"),
                "bronze_record_hash": r.get("bronze_record_hash"),
            }
        )

    # Defensive de-duplication preserves the fact grain even if upstream source
    # delivers duplicate deviation records.  Since build_silver is already keyed
    # by deviation_id this is normally a no-op.
    fact_by_deviation = {row["deviation_id"]: row for row in fact_rows}
    fact_rows = _sort_deviations(fact_by_deviation.values())

    _write_json(client, cfg.GOLD_FACT_PREFIX, fact_rows)
    return fact_rows


def run_pipeline(client=None):
    client = client or _s3()
    build_silver(client)
    build_gold(client)
    return True
