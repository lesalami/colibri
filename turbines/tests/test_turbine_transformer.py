import pytest
from pyspark.sql import SparkSession
from src.utils.turbine_transformer import TurbineTransformer

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-tests") \
        .getOrCreate()


def test_clean_bronze_removes_null_ids(spark):
    data = [
        (None, 10.5, 2000),
        ("T1", 12.0, 2500)
    ]
    df = spark.createDataFrame(data, ["turbine_id", "wind_speed", "power_output"])

    result = TurbineTransformer.clean_bronze(df)

    assert result.count() == 1


def test_power_category_assignment(spark):
    data = [
        ("T1", 10.0, 500),
        ("T2", 10.0, 2000),
        ("T3", 10.0, 4000),
    ]
    df = spark.createDataFrame(data, ["turbine_id", "wind_speed", "power_output"])

    result = TurbineTransformer.add_power_category(df)

    categories = [row["power_category"] for row in result.collect()]

    assert categories == ["LOW", "MEDIUM", "HIGH"]
    

def test_anomaly_flagging(spark):

    data = [
        ("1", "T1", "2024-01-01 00:00:00", 5.0),
        ("1", "T1", "2024-01-01 01:00:00", 5.0),
        ("1", "T1", "2024-01-01 02:00:00", 20.0),  # anomaly
    ]

    df = spark.createDataFrame(
        data,
        ["data_group", "turbine_id", "timestamp", "power_output_mw"]
    ).withColumn("timestamp", to_timestamp("timestamp")) \
     .withColumn("date", to_date("timestamp"))

    summary = TurbineTransformer.calculate_daily_summary(df)

    joined = df.join(summary, ["data_group", "turbine_id", "date"])

    flagged = TurbineTransformer.flag_anomalies(joined)

    anomalies = flagged.filter(col("is_anomaly") == True)

    assert anomalies.count() == 1