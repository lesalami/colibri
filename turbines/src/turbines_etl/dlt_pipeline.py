import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Unity Catalog Volume Path
input_path = spark.conf.get(
    "input_path",
    "/Volumes/workspace/default/turbines"
)

# schema = StructType([
#     StructField("timestamp", StringType(), True),
#     StructField("turbine_id", StringType(), True),
#     StructField("wind_speed", DoubleType(), True),
#     StructField("wind_direction", DoubleType(), True),
#     StructField("power_output_mw", DoubleType(), True)
# ])

# =====================================================
# BRONZE LAYER
# =====================================================

# @dlt.table(
#     name="bronze_turbine_data",
#     comment="Raw ingestion of turbine CSV files from UC Volume"
# )
# def bronze():

#     df = (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "true")
#         # .option("cloudFiles.inferColumnTypes", "false")
#         # .schema(schema)
#         .load(input_path)
#     )

#     # return (
#     #     df.withColumn(
#     #         "data_group",
#     #         regexp_extract(input_file_name(), "data_group_(\\d+)", 1)
#     #     )
#     #     .withColumn("ingestion_timestamp", current_timestamp())
#     # )
#     return TurbineTransformer.clean_data(df)

@dlt.table(
    name="bronze_turbine_data",
    comment="Raw ingestion of turbine CSV files from UC Volume"
)
def bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(input_path)
    )

    df = df.withColumn(
        "data_group",
        regexp_extract(input_file_name(), "data_group_(\\d+)", 1)
    )

    return TurbineTransformer.clean_data(df)

# =====================================================
# SILVER LAYER (Clean + Validate)
# =====================================================

@dlt.table(name="silver_turbine_data")
@dlt.expect_or_drop("valid_power_range", "power_output_mw BETWEEN 0 AND 10")
@dlt.expect_or_drop("valid_wind_speed", "wind_speed BETWEEN 0 AND 60")
@dlt.expect_or_drop("not_null_timestamp", "timestamp IS NOT NULL")
def silver():

    df = dlt.read("bronze_turbine_data")

    return (
        df.withColumn("timestamp", to_timestamp("timestamp"))
          .dropDuplicates(["timestamp", "turbine_id"])
    )

# =====================================================
# QUARANTINE TABLE
# =====================================================

@dlt.table(
    name="silver_quarantine_turbine_data",
    comment="Rows failing validation rules"
)
def quarantine():

    df = dlt.read("bronze_turbine_data")

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

@dlt.table(name="gold_turbine_summary")
def gold_summary():

    df = dlt.read("silver_turbine_data") \
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

@dlt.table(name="gold_turbine_anomalies")
def gold_anomalies():

    df = dlt.read("silver_turbine_data") \
            .withColumn("date", to_date("timestamp"))

    summary = dlt.read("gold_turbine_summary")

    joined = df.join(
        summary,
        ["data_group", "turbine_id", "date"]
    )

    return TurbineTransformer.flag_anomalies(joined) \
            .filter(col("is_anomaly") == True)