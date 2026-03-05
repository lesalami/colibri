import sys
import os

# Add src directory to sys.path if not already present
src_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from pyspark import pipelines as dp
from pyspark.sql.functions import regexp_extract, current_timestamp, to_timestamp, col, to_date, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from utils.turbine_transformer import TurbineTransformer

# Unity Catalog Volume Path
input_path = "/Volumes/workspace/dev_lesalami_wind_turbines/turbines"


# =====================================================
# BRONZE LAYER
# =====================================================

@dp.table(
    name="bronze_turbine_data",
    comment="Raw ingestion of turbine CSV files from UC Volume"
)
def bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("checkpointLocation", "/Volumes/workspace/dev_lesalami_wind_turbines/checkpoints/bronze_autoloader")
        .load(input_path)
    )

    # df = df.withColumn(
    #     "data_group",
    #     regexp_extract(col("_metadata.file_path"), "data_group_(\\d+)", 1)
    # )
    return (
    df.withColumn(
        "data_group",
        regexp_extract(col("_metadata.file_path"), "data_group_(\\d+)", 1)
    )
    .withColumn("ingestion_timestamp", current_timestamp())
)

# =====================================================
# SILVER LAYER (Clean + Validate)
# =====================================================

@dp.table(name="silver_turbine_data", comment="Raw ingestion of turbine CSV files from UC Volume")
@dp.expect_or_drop("valid_power_range", "power_output_mw BETWEEN 0 AND 10")
@dp.expect_or_drop("valid_wind_speed", "wind_speed BETWEEN 0 AND 60")
@dp.expect_or_drop("not_null_timestamp", "timestamp IS NOT NULL")
def silver():

    df = dp.read_stream("bronze_turbine_data")

    return (
        df.withColumn("timestamp", to_timestamp("timestamp"))
          .dropDuplicates(["timestamp", "turbine_id"])
    )

# =====================================================
# QUARANTINE TABLE
# =====================================================

@dp.table(
    name="silver_quarantine_turbine_data",
    comment="Rows failing validation rules"
)
def quarantine():

    df = dp.read_stream("bronze_turbine_data")

    return df.filter(
        (col("power_output_mw") < 0) |
        (col("power_output_mw") > 10) |
        (col("wind_speed") < 0) |
        (col("wind_speed") > 60) |
        (col("timestamp").isNull())
    )

# =====================================================
# GOLD LAYER - DAILY SUMMARY
# =====================================================

@dp.table(name="gold_turbine_summary")
def gold_summary():

    df = dp.read_stream("silver_turbine_data") \
            .withColumn("date", to_date("timestamp"))

    summary = TurbineTransformer.calculate_daily_summary(df)

    joined = df.join(summary, ["data_group", "turbine_id", "date"])

    flagged = TurbineTransformer.flag_anomalies(joined)

    anomaly_counts = (
        flagged.groupBy("data_group", "turbine_id", "date")
               .agg(sum(col("is_anomaly").cast("int")).alias("anomaly_count"))
    )

    return summary.join(
        anomaly_counts,
        ["data_group", "turbine_id", "date"],
        "left"
    ).fillna({"anomaly_count": 0})

# =====================================================
# GOLD LAYER - ANOMALIES
# =====================================================

@dp.table(name="gold_turbine_anomalies")
def gold_anomalies():

    df = dp.read_stream("silver_turbine_data") \
            .withColumn("date", to_date("timestamp"))

    summary = dp.read("gold_turbine_summary")

    joined = df.join(
        summary,
        ["data_group", "turbine_id", "date"]
    )

    return TurbineTransformer.flag_anomalies(joined) \
            .filter(col("is_anomaly") == True)