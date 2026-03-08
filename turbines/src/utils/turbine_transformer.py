from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class TurbineTransformer:
    """Utility class for turbine telemetry transformation and business logic.

    This class centralizes reusable logic for:
    - schema standardization
    - deduplication
    - data quality validation and quarantine
    - daily summary aggregation
    - anomaly flagging
    - missing interval detection
    - quality reporting
    - short-gap forward filling for imputed datasets

    All methods are static so they can be reused easily in DLT pipelines
    and unit-tested independently.
    """

    @staticmethod
    def standardize_bronze(df: DataFrame) -> DataFrame:
        """Standardize raw Bronze data into typed telemetry columns.

        Expected input columns:
        - timestamp
        - turbine_id
        - wind_speed
        - wind_direction
        - power_output

        Output columns include:
        - timestamp as timestamp type
        - turbine_id as string
        - wind_speed as double
        - wind_direction as double
        - power_output as double
        - date derived from timestamp

        Args:
            df: Raw Bronze DataFrame.

        Returns:
            A standardized DataFrame with typed columns and a derived `date`.
        """
        return (
            df.withColumn("timestamp", F.to_timestamp("timestamp"))
            .withColumn("turbine_id", F.col("turbine_id").cast("string"))
            .withColumn("wind_speed", F.col("wind_speed").cast("double"))
            .withColumn("wind_direction", F.col("wind_direction").cast("double"))
            .withColumn("power_output", F.col("power_output").cast("double"))
            .withColumn("date", F.to_date("timestamp"))
        )

    @staticmethod
    def deduplicate(df: DataFrame) -> DataFrame:
        """Remove duplicate turbine readings by turbine and timestamp.

        Args:
            df: Input DataFrame containing telemetry rows.

        Returns:
            A DataFrame with duplicate `(turbine_id, timestamp)` rows removed.
        """
        return df.dropDuplicates(["turbine_id", "timestamp"])

    @staticmethod
    def split_valid_and_quarantine(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Split telemetry rows into valid and quarantined datasets.

        Invalid rows are those that fail basic quality rules, including:
        - missing required values
        - negative or unrealistic wind speed
        - wind direction outside 0-360
        - power output outside allowed range

        A `quarantine_reason` column is added to the quarantined dataset.

        Args:
            df: Input DataFrame containing standardized telemetry rows.

        Returns:
            A tuple of:
            - valid_df: rows passing validation
            - quarantined_df: rows failing validation with `quarantine_reason`
        """
        invalid_condition = (
            F.col("timestamp").isNull()
            | F.col("turbine_id").isNull()
            | F.col("wind_speed").isNull()
            | F.col("wind_direction").isNull()
            | F.col("power_output").isNull()
            | (F.col("wind_speed") < 0)
            | (F.col("wind_speed") > 60)
            | (F.col("wind_direction") < 0)
            | (F.col("wind_direction") > 360)
            | (F.col("power_output") < 0)
            | (F.col("power_output") > 10)
        )

        quarantine_reason = (
            F.when(F.col("timestamp").isNull(), F.lit("missing_timestamp"))
            .when(F.col("turbine_id").isNull(), F.lit("missing_turbine_id"))
            .when(F.col("wind_speed").isNull(), F.lit("missing_wind_speed"))
            .when(F.col("wind_direction").isNull(), F.lit("missing_wind_direction"))
            .when(F.col("power_output").isNull(), F.lit("missing_power_output"))
            .when(F.col("wind_speed") < 0, F.lit("negative_wind_speed"))
            .when(F.col("wind_speed") > 60, F.lit("wind_speed_out_of_range"))
            .when(F.col("wind_direction") < 0, F.lit("negative_wind_direction"))
            .when(F.col("wind_direction") > 360, F.lit("wind_direction_out_of_range"))
            .when(F.col("power_output") < 0, F.lit("negative_power_output"))
            .when(F.col("power_output") > 10, F.lit("power_output_out_of_range"))
        )

        quarantined_df = df.filter(invalid_condition).withColumn(
            "quarantine_reason", quarantine_reason
        )
        valid_df = df.filter(~invalid_condition)

        return valid_df, quarantined_df

    @staticmethod
    def calculate_daily_summary(df: DataFrame) -> DataFrame:
        """Calculate daily power summary statistics per turbine.

        Aggregations are calculated by:
        - data_group
        - turbine_id
        - date

        Metrics produced:
        - min_power
        - max_power
        - avg_power
        - std_power
        - reading_count

        Args:
            df: Valid telemetry DataFrame containing `power_output` and `date`.

        Returns:
            A DataFrame with daily summary statistics per turbine.
        """
        return (
            df.groupBy("data_group", "turbine_id", "date")
            .agg(
                F.min("power_output").alias("min_power"),
                F.max("power_output").alias("max_power"),
                F.avg("power_output").alias("avg_power"),
                F.stddev("power_output").alias("std_power"),
                F.count("*").alias("reading_count"),
            )
        )

    @staticmethod
    def flag_anomalies(df: DataFrame) -> DataFrame:
        """Flag statistical anomalies using the 2-standard-deviation rule.

        A row is marked as anomalous when:

            abs(power_output - avg_power) > 2 * std_power

        If `std_power` is null, the row is marked as non-anomalous.

        Args:
            df: DataFrame containing `power_output`, `avg_power`, and `std_power`.

        Returns:
            The input DataFrame with an added boolean column `is_anomaly`.
        """
        return df.withColumn(
            "is_anomaly",
            F.when(F.col("std_power").isNull(), F.lit(False)).otherwise(
                F.abs(F.col("power_output") - F.col("avg_power"))
                > 2 * F.col("std_power")
            ),
        )

    @staticmethod
    def calculate_anomaly_counts(df: DataFrame) -> DataFrame:
        """Count anomalies per turbine per day.

        Args:
            df: DataFrame containing `is_anomaly`, grouped dimensions, and date.

        Returns:
            A DataFrame with one row per turbine/day and `anomaly_count`.
        """
        return (
            df.groupBy("data_group", "turbine_id", "date")
            .agg(F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"))
        )

    @staticmethod
    def calculate_missing_intervals(
        df: DataFrame, frequency: str = "1 hour"
    ) -> DataFrame:
        """Detect missing expected timestamps for each turbine/day.

        This method generates the expected time sequence between the first and
        last observed timestamp for each turbine/day and identifies timestamps
        that are missing from the actual telemetry.

        Args:
            df: Valid telemetry DataFrame containing `timestamp`, `date`,
                `data_group`, and `turbine_id`.
            frequency: Interval between expected readings. Defaults to `"1 hour"`.

        Returns:
            A DataFrame containing missing timestamps with columns:
            - data_group
            - turbine_id
            - date
            - missing_timestamp
        """
        bounds = (
            df.groupBy("data_group", "turbine_id", "date")
            .agg(
                F.min("timestamp").alias("min_ts"),
                F.max("timestamp").alias("max_ts"),
            )
        )

        expected = (
            bounds.withColumn(
                "expected_timestamp",
                F.explode(F.expr(f"sequence(min_ts, max_ts, interval {frequency})")),
            )
            .select("data_group", "turbine_id", "date", "expected_timestamp")
        )

        actual = df.select(
            "data_group",
            "turbine_id",
            "date",
            F.col("timestamp").alias("expected_timestamp"),
        )

        return (
            expected.join(
                actual,
                ["data_group", "turbine_id", "date", "expected_timestamp"],
                "left_anti",
            ).withColumnRenamed("expected_timestamp", "missing_timestamp")
        )

    @staticmethod
    def calculate_quality_summary(
        valid_df: DataFrame,
        quarantine_df: DataFrame,
        missing_df: DataFrame,
    ) -> DataFrame:
        """Build a daily quality summary across valid, quarantined, and missing data.

        The summary is produced by:
        - data_group
        - date

        Metrics produced:
        - valid_record_count
        - quarantine_record_count
        - missing_interval_count

        Args:
            valid_df: DataFrame of valid telemetry rows.
            quarantine_df: DataFrame of quarantined telemetry rows.
            missing_df: DataFrame of missing intervals.

        Returns:
            A DataFrame containing daily data quality metrics.
        """
        valid_counts = valid_df.groupBy("data_group", "date").agg(
            F.count("*").alias("valid_record_count")
        )

        quarantine_counts = quarantine_df.groupBy("data_group", "date").agg(
            F.count("*").alias("quarantine_record_count")
        )

        missing_counts = missing_df.groupBy("data_group", "date").agg(
            F.count("*").alias("missing_interval_count")
        )

        return (
            valid_counts.join(quarantine_counts, ["data_group", "date"], "full")
            .join(missing_counts, ["data_group", "date"], "full")
            .fillna(
                {
                    "valid_record_count": 0,
                    "quarantine_record_count": 0,
                    "missing_interval_count": 0,
                }
            )
        )

    @staticmethod
    def forward_fill_short_gaps(
        df: DataFrame, max_gap_hours: int = 3
    ) -> DataFrame:
        """Forward-fill short consecutive telemetry gaps.

        This function assumes missing interval rows have already been inserted
        into the dataset with null values for:
        - power_output
        - wind_speed
        - wind_direction

        Only gaps up to `max_gap_hours` consecutive intervals are filled.
        Longer gaps remain null so they can still be treated as genuine data loss.

        The function also adds:
        - `is_imputed`: True when a row was filled by forward fill

        Expected input columns:
        - data_group
        - turbine_id
        - timestamp
        - power_output
        - wind_speed
        - wind_direction

        Args:
            df: Combined DataFrame containing real and synthetic missing rows.
            max_gap_hours: Maximum consecutive missing intervals to impute.

        Returns:
            A DataFrame with short gaps forward-filled and `is_imputed` added.
        """
        ordered_window = Window.partitionBy("data_group", "turbine_id").orderBy(
            "timestamp"
        )
        cumulative_window = ordered_window.rowsBetween(
            Window.unboundedPreceding, 0
        )

        df = df.withColumn(
            "is_missing_row",
            F.when(
                F.col("power_output").isNull()
                & F.col("wind_speed").isNull()
                & F.col("wind_direction").isNull(),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        df = df.withColumn(
            "gap_group",
            F.sum(F.when(~F.col("is_missing_row"), 1).otherwise(0)).over(
                ordered_window
            ),
        )

        df = df.withColumn(
            "gap_size",
            F.sum(F.when(F.col("is_missing_row"), 1).otherwise(0)).over(
                Window.partitionBy("data_group", "turbine_id", "gap_group")
            ),
        )

        df = df.withColumn(
            "last_power_output",
            F.last("power_output", ignorenulls=True).over(cumulative_window),
        )
        df = df.withColumn(
            "last_wind_speed",
            F.last("wind_speed", ignorenulls=True).over(cumulative_window),
        )
        df = df.withColumn(
            "last_wind_direction",
            F.last("wind_direction", ignorenulls=True).over(cumulative_window),
        )

        df = df.withColumn(
            "power_output",
            F.when(
                F.col("is_missing_row") & (F.col("gap_size") <= max_gap_hours),
                F.col("last_power_output"),
            ).otherwise(F.col("power_output")),
        )

        df = df.withColumn(
            "wind_speed",
            F.when(
                F.col("is_missing_row") & (F.col("gap_size") <= max_gap_hours),
                F.col("last_wind_speed"),
            ).otherwise(F.col("wind_speed")),
        )

        df = df.withColumn(
            "wind_direction",
            F.when(
                F.col("is_missing_row") & (F.col("gap_size") <= max_gap_hours),
                F.col("last_wind_direction"),
            ).otherwise(F.col("wind_direction")),
        )

        df = df.withColumn(
            "is_imputed",
            F.when(
                F.col("is_missing_row") & (F.col("gap_size") <= max_gap_hours),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        return df.drop(
            "last_power_output",
            "last_wind_speed",
            "last_wind_direction",
            "gap_group",
            "gap_size",
        )