# S3 layout
# ---------------------------------------------------------------------------
BUCKET = "sephora-target"

# RAW_PREFIX     = f"{BUCKET}/raw/orders"          # original source CSVs land here (MySQL/SQLServer/API dumps)
RAW_PREFIX = f"{BUCKET}/raw"
BRONZE_PREFIX  = f"{BUCKET}/Raw data bronze"       # raw data copied 1:1 into Delta, ingestion metadata added
SILVER_PREFIX  = f"{BUCKET}/clean_silver"       # cleaned, standardized, deduplicated, FK-validated
QUARANTINE_PREFIX = f"{BUCKET}/quarantine"  # rows that fail validation, kept for DQ reporting
GOLD_PREFIX    = f"{BUCKET}/gold"         # star-schema dims + facts, ready for Power BI
DQ_METRICS_PATH = f"{BUCKET}/dq_metrics"  # one row per pipeline run per table with pass/fail counts