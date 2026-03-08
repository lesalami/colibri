"""
Lakeflow Declarative Pipeline for wind turbine telemetry.

Design notes
------------
This pipeline is intentionally implemented as a batch-style full reprocess
pattern in Bronze because the source system appends rows to existing CSV files.
That makes immutable-file streaming semantics a poor fit.

Pipeline layers
---------------
Bronze
    Full snapshot ingestion of raw CSV files from a Unity Catalog volume.

Silver
    - standardize and deduplicate telemetry
    - quarantine invalid telemetry
    - detect missing expected telemetry intervals
    - create an imputed dataset by forward-filling short gaps

Gold
    - daily turbine summary statistics
    - anomaly detection using the 2-standard-deviation rule
    - operational data quality summary

Expected runtime configuration
------------------------------
The pipeline expects `input_path` to be passed in via bundle configuration, e.g.

    configuration:
      input_path: /Volumes/workspace/dev_<username>_turbines/turbines
"""
import sys
import os

# Add src directory to sys.path if not already present
src_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# from turbines_etl.utils.turbine_transformer import TurbineTransformer
from utils.turbine_transformer import TurbineTransformer

# Unity Catalog volume path containing the CSV source files.
input_path: str = spark.conf.get("input_path")


# =====================================================
# BRONZE LAYER
# =====================================================

@dp.table(
    name="bronze_turbine_data",
    comment="Daily full snapshot ingestion of raw turbine CSV files from a Unity Catalog volume."
)
def bronze_turbine_data() -> DataFrame:
    """Ingest the current full state of the source CSV files.

    Why batch instead of streaming
    ------------------------------
    The source files are appended in place. Since the same file is updated
    repeatedly, a full reprocess pattern is safer and more deterministic than
    relying on immutable-file streaming semantics.

    What this table contains
    ------------------------
    - raw CSV rows
    - data_group extracted from the source file name
    - source_file path for lineage/auditability
    - snapshot_ingestion_ts showing when this snapshot was taken

    Returns:
        DataFrame: Raw Bronze snapshot with source metadata attached.
    """
    df: DataFrame = (
        spark.read
        .format("csv")
        .option("header", "true")
        .load(input_path)
    )

    return (
        df.withColumn(
            "data_group",
            F.regexp_extract(F.col("_metadata.file_path"), r"data_group_(\d+)", 1)
        )
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("snapshot_ingestion_ts", F.current_timestamp())
        .withColumn("pipeline_run_id", F.expr("uuid()"))
    )


# =====================================================
# SILVER LAYER - VALIDATED DATA
# =====================================================

@dp.table(
    name="silver_turbine_data",
    comment="Validated turbine telemetry with enforced data quality rules."
)

@dp.expect_or_drop(
    "valid_timestamp",
    "timestamp IS NOT NULL"
)

@dp.expect_or_drop(
    "valid_wind_speed_range",
    "wind_speed BETWEEN 0 AND 60"
)

@dp.expect_or_drop(
    "valid_wind_direction_range",
    "wind_direction BETWEEN 0 AND 360"
)

@dp.expect_or_drop(
    "valid_power_output_range",
    "power_output BETWEEN 0 AND 10"
)

def silver_turbine_data() -> DataFrame:
    """
    Silver table containing validated turbine telemetry.

    DLT expectations enforce core quality rules and automatically track
    metrics about failed records.

    Additional deduplication and transformation logic is delegated to the
    TurbineTransformer utility class.
    """

    df = dp.read("bronze_turbine_data")

    df = TurbineTransformer.standardize_bronze(df)

    df = TurbineTransformer.deduplicate(df)

    return df


# =====================================================
# SILVER LAYER - QUARANTINE
# =====================================================

@dp.table(
    name="silver_quarantine_turbine_data",
    comment="Invalid turbine telemetry rows isolated for investigation and audit."
)
def silver_quarantine_turbine_data() -> DataFrame:
    """Create the Silver quarantine table.

    Purpose
    -------
    Invalid rows are preserved instead of being silently dropped so that:
    - data quality issues remain auditable
    - support teams can investigate sensor/system issues
    - analytics remain based only on trusted data

    Examples of quarantined rows
    ----------------------------
    - missing timestamp
    - missing turbine_id
    - negative wind speed
    - wind direction outside 0-360
    - power output outside allowed range

    Returns:
        DataFrame: Invalid rows with `quarantine_reason`.
    """
    df: DataFrame = dp.read("bronze_turbine_data")
    df = TurbineTransformer.standardize_bronze(df)
    df = TurbineTransformer.deduplicate(df)

    _, quarantined_df = TurbineTransformer.split_valid_and_quarantine(df)
    return quarantined_df


# =====================================================
# SILVER LAYER - MISSING INTERVALS
# =====================================================

@dp.table(
    name="silver_missing_intervals",
    comment="Expected turbine telemetry timestamps that were not received, indicating likely sensor/turbine reporting gaps."
)
def silver_missing_intervals() -> DataFrame:
    """Detect missing expected telemetry intervals.

    Purpose
    -------
    The project brief mentions that sensor malfunctions can cause missing
    entries. Rather than hiding that issue, this table makes the missing
    timestamps explicit.

    Logic
    -----
    For each `(data_group, turbine_id, date)`:
    - find the minimum and maximum observed timestamp
    - generate the expected timestamp sequence at hourly intervals
    - compare expected timestamps with actual timestamps
    - output any missing timestamps

    Returns:
        DataFrame: Missing expected timestamps per turbine/day.
    """
    df: DataFrame = dp.read("silver_turbine_data")
    return TurbineTransformer.calculate_missing_intervals(df, frequency="1 hour")


# =====================================================
# SILVER LAYER - IMPUTED DATA
# =====================================================

@dp.table(
    name="silver_imputed_turbine_data",
    comment="Analytic-friendly turbine telemetry with short gaps forward-filled while preserving the original validated dataset separately."
)
def silver_imputed_turbine_data() -> DataFrame:
    """Create a Silver dataset with short gaps forward-filled.

    Why this table exists
    ---------------------
    Some downstream analytical use cases benefit from a continuous hourly time
    series. However, we should not overwrite the validated source truth in
    `silver_turbine_data`.

    Approach
    --------
    1. Read valid telemetry from `silver_turbine_data`.
    2. Read missing timestamps from `silver_missing_intervals`.
    3. Materialize synthetic placeholder rows for those missing timestamps.
    4. Union actual rows and synthetic rows.
    5. Forward-fill short consecutive gaps only.

    Important
    ---------
    Long gaps are intentionally left unfilled so they remain visible as true
    data loss. The utility function controls this using `max_gap_hours`.

    Returns:
        DataFrame: Telemetry including imputed rows, with `is_imputed` flag.
    """
    valid_df: DataFrame = dp.read("silver_turbine_data")
    missing_df: DataFrame = dp.read("silver_missing_intervals")

    missing_rows: DataFrame = (
        missing_df.withColumnRenamed("missing_timestamp", "timestamp")
        .withColumn("wind_speed", F.lit(None).cast("double"))
        .withColumn("wind_direction", F.lit(None).cast("double"))
        .withColumn("power_output", F.lit(None).cast("double"))
        .withColumn("is_missing_row", F.lit(True))
    )

    valid_rows: DataFrame = valid_df.withColumn("is_missing_row", F.lit(False))

    combined: DataFrame = valid_rows.unionByName(
        missing_rows,
        allowMissingColumns=True
    )

    return TurbineTransformer.forward_fill_short_gaps(
        combined,
        max_gap_hours=3
    )


# =====================================================
# GOLD LAYER - DAILY SUMMARY
# =====================================================

@dp.table(
    name="gold_turbine_summary",
    comment="Daily turbine summary statistics with anomaly counts per turbine."
)
def gold_turbine_summary() -> DataFrame:
    """Build daily summary statistics for each turbine.

    Metrics produced
    ----------------
    - min_power
    - max_power
    - avg_power
    - std_power
    - reading_count
    - anomaly_count

    Input choice
    ------------
    This summary reads from `silver_imputed_turbine_data` so that short,
    acceptable telemetry gaps can be filled for analytics. This improves
    continuity without altering the validated source-truth Silver table.

    Returns:
        DataFrame: Daily summary metrics per turbine/day.
    """
    df: DataFrame = dp.read("silver_imputed_turbine_data")

    summary: DataFrame = TurbineTransformer.calculate_daily_summary(df)

    flagged: DataFrame = TurbineTransformer.flag_anomalies(
        df.join(summary, ["data_group", "turbine_id", "date"], "inner")
    )

    anomaly_counts: DataFrame = TurbineTransformer.calculate_anomaly_counts(flagged)

    return (
        summary.join(anomaly_counts, ["data_group", "turbine_id", "date"], "left")
        .fillna({"anomaly_count": 0})
    )


# =====================================================
# GOLD LAYER - ANOMALIES
# =====================================================

@dp.table(
    name="gold_turbine_anomalies",
    comment="Detailed turbine readings identified as anomalies using the 2-standard-deviation rule."
)
def gold_turbine_anomalies() -> DataFrame:
    """Create the detailed Gold anomaly table.

    Rule used
    ---------
    A reading is marked as anomalous when:

        abs(power_output - avg_power) > 2 * std_power

    This implements the anomaly rule specified in the project brief.

    Returns:
        DataFrame: Detailed anomalous rows including `is_anomaly = true`.
    """
    df: DataFrame = dp.read("silver_imputed_turbine_data")
    summary: DataFrame = dp.read("gold_turbine_summary")

    joined: DataFrame = df.join(
        summary,
        ["data_group", "turbine_id", "date"],
        "inner"
    )

    flagged: DataFrame = TurbineTransformer.flag_anomalies(joined)

    return flagged.filter(F.col("is_anomaly") == True)


# =====================================================
# GOLD LAYER - DATA QUALITY SUMMARY
# =====================================================

@dp.table(
    name="gold_turbine_data_quality_summary",
    comment="Daily operational summary of valid telemetry, quarantined records, and missing intervals."
)
def gold_turbine_data_quality_summary() -> DataFrame:
    """Create a daily operational data quality summary.

    Purpose
    -------
    This table is for operational observability rather than business analytics.
    It helps answer:
    - how many valid records arrived
    - how many records were quarantined
    - how many expected intervals were missing

    Returns:
        DataFrame: Daily quality metrics by data group and date.
    """
    valid_df: DataFrame = dp.read("silver_turbine_data")
    quarantine_df: DataFrame = dp.read("silver_quarantine_turbine_data")
    missing_df: DataFrame = dp.read("silver_missing_intervals")

    return TurbineTransformer.calculate_quality_summary(
        valid_df=valid_df,
        quarantine_df=quarantine_df,
        missing_df=missing_df
    )