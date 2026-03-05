from pyspark.sql.functions import *
from pyspark.sql import DataFrame


class TurbineTransformer:

    @staticmethod
    def clean_data(df: DataFrame) -> DataFrame:
        """
        Minimal cleaning only.
        Do NOT remove outliers here.
        """

        return (
            df.filter(col("turbine_id").isNotNull())
              .withColumn("wind_speed", col("wind_speed").cast("double"))
              .withColumn("wind_direction", col("wind_direction").cast("double"))
              .withColumn("power_output", col("power_output").cast("double"))
        )


    @staticmethod
    def calculate_daily_summary(df: DataFrame) -> DataFrame:
        return (
            df.groupBy("data_group", "turbine_id", "date")
              .agg(
                  min("power_output").alias("min_power"),
                  max("power_output").alias("max_power"),
                  avg("power_output").alias("avg_power"),
                  stddev("power_output").alias("std_power")
              )
        )


    @staticmethod
    def flag_anomalies(df: DataFrame) -> DataFrame:
        """
        Adds anomaly boolean column
        """

        return df.withColumn(
            "is_anomaly",
            abs(col("power_output") - col("avg_power")) > 2 * col("std_power")
        )