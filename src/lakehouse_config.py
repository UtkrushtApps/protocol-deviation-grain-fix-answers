"""Local configuration for the protocol-deviation Delta lakehouse scaffold.

Represents Databricks-style medallion locations backed by MinIO object storage.
No real secrets; values mirror the local docker-compose MinIO service.
"""
import os

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

LAKEHOUSE_BUCKET = "clinical-lakehouse"

# Logical medallion object-storage prefixes (Delta-style table paths).
BRONZE_PREFIX = "bronze/protocol_deviation"
SILVER_DEVIATION_PREFIX = "silver/protocol_deviation"
SILVER_ACTION_PREFIX = "silver/protocol_deviation_action"
SILVER_FLAT_PREFIX = "silver/protocol_deviation_flat"
GOLD_FACT_PREFIX = "gold/fact_protocol_deviation"

# Source fixture landed into bronze.
SOURCE_FIXTURE = os.path.join(os.path.dirname(__file__), "..", "fixtures", "source_data.json")

SOURCE_SYSTEM = "EDC_PRIMARY"

# Required (NOT NULL) fields that a curated deviation record must carry.
DEVIATION_REQUIRED_FIELDS = ["deviation_id", "study_id", "deviation_date"]
