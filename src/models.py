"""Schema, grain, and validation helpers for the protocol deviation product.

Defines the metadata/lineage columns and required-field expectations used by
the curated layers. Grain conventions are described here so the curated and
gold tables can express what a single row means.
"""
from src import lakehouse_config as cfg

# Grain documentation used by the pipeline and by downstream readers.
DEVIATION_GRAIN = "one row per distinct protocol deviation"
ACTION_GRAIN = "one row per corrective action for a protocol deviation"

# Standard lineage / metadata columns expected on curated rows.
METADATA_COLUMNS = ["source_system", "ingestion_ts", "record_hash"]

# Additional auditable lineage columns that tie silver/gold rows back to the
# bronze object and the specific bronze source record from which they came.
LINEAGE_COLUMNS = ["bronze_object_key", "bronze_record_hash"]

# Business keys.
DEVIATION_BUSINESS_KEY = "deviation_id"
ACTION_BUSINESS_KEY = "action_id"


def _present(value):
    """Return True when a field value satisfies a simple required-field check."""
    return value is not None and value != ""


def deviation_required_ok(row):
    """Return True only if all required deviation identifying fields are present."""
    for field in cfg.DEVIATION_REQUIRED_FIELDS:
        if not _present(row.get(field)):
            return False
    return True


def action_required_ok(row):
    """Return True only if an action has enough keys to stand as action grain.

    The source fixture supplies action_id for all real corrective actions.  Empty
    corrective_actions arrays are represented by zero action rows, not by a null
    placeholder action.
    """
    return _present(row.get("deviation_id")) and _present(row.get("action_id"))


def deviation_columns():
    return [
        "deviation_id",
        "study_id",
        "subject_id",
        "site_id",
        "deviation_date",
        "deviation_category",
        "severity",
    ] + METADATA_COLUMNS + LINEAGE_COLUMNS


def action_columns():
    return [
        "action_id",
        "deviation_id",
        "action_ordinal",
        "action_description",
        "action_status",
    ] + METADATA_COLUMNS + LINEAGE_COLUMNS


def gold_fact_columns():
    return deviation_columns()
