import pytest
from pyspark.sql import SparkSession
from turbines_etl.utils.turbine_transformer import TurbineTransformer
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("turbine-transformer-tests")
        .getOrCreate()
    )


def test_standardize_types_and_add_date(spark):
    data = [
        ("2026-01-01 00:00:00", "T1", "10.0", "180.0", "5.2", "1")
    ]
    df = spark.createDataFrame(
        data,
        ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output_mw", "data_group"]
    )

    result = TurbineTransformer.standardize_types(df)
    result = TurbineTransformer.add_date(result)

    row = result.collect()[0]
    assert row["timestamp"] is not None
    assert isinstance(row["wind_speed"], float)
    assert row["date"] is not None


def test_deduplicate(spark):
    data = [
        ("2026-01-01 00:00:00", "T1", 10.0, 180.0, 5.2, "1"),
        ("2026-01-01 00:00:00", "T1", 10.0, 180.0, 5.2, "1")
    ]
    df = spark.createDataFrame(
        data,
        ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output_mw", "data_group"]
    )

    df = TurbineTransformer.standardize_types(df)
    result = TurbineTransformer.deduplicate(df)

    assert result.count() == 1


def test_split_valid_and_quarantine(spark):
    data = [
        ("2026-01-01 00:00:00", "T1", 10.0, 180.0, 5.2, "1"),
        ("2026-01-01 01:00:00", "T2", -1.0, 180.0, 4.0, "1"),
        ("2026-01-01 02:00:00", "T3", 9.0, 180.0, 20.0, "1")
    ]
    df = spark.createDataFrame(
        data,
        ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output_mw", "data_group"]
    )

    df = TurbineTransformer.standardize_types(df)
    df = TurbineTransformer.add_date(df)
    valid_df, quarantine_df = TurbineTransformer.split_valid_and_quarantine(df)

    assert valid_df.count() == 1
    assert quarantine_df.count() == 2


def test_calculate_daily_summary(spark):
    data = [
        ("2026-01-01 00:00:00", "T1", 10.0, 180.0, 5.0, "1"),
        ("2026-01-01 01:00:00", "T1", 12.0, 182.0, 7.0, "1")
    ]
    df = spark.createDataFrame(
        data,
        ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output_mw", "data_group"]
    )

    df = TurbineTransformer.standardize_types(df)
    df = TurbineTransformer.add_date(df)

    summary = TurbineTransformer.calculate_daily_summary(df).collect()[0]

    assert summary["min_power"] == 5.0
    assert summary["max_power"] == 7.0
    assert round(summary["avg_power"], 2) == 6.0


def test_flag_anomalies(spark):
    data = [
        ("1", "T1", "2026-01-01", 5.0, 10.0, 20.0, 8.0),   # not anomaly
        ("1", "T1", "2026-01-01", 20.0, 10.0, 20.0, 2.0)   # anomaly
    ]
    df = spark.createDataFrame(
        data,
        ["data_group", "turbine_id", "date", "power_output_mw", "avg_power", "max_power", "std_power"]
    )

    flagged = TurbineTransformer.flag_anomalies(df)
    anomaly_count = flagged.filter(F.col("is_anomaly") == True).count()

    assert anomaly_count == 1