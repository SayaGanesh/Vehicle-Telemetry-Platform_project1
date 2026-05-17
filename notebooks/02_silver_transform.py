# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Silver Layer: Cleanse & Validate Telemetry
# MAGIC
# MAGIC Reads Bronze Delta table, applies schema enforcement, null handling,
# MAGIC deduplication, unit validation, and anomaly flagging.
# MAGIC Writes clean records to Silver Delta table partitioned by date and vehicle.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit, row_number, current_timestamp,
    regexp_replace, trim, upper
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

spark = SparkSession.builder.appName("VehicleTelemetry_Silver").getOrCreate()

# COMMAND ----------

STORAGE_ACCOUNT = "yourstorageaccount"
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/vehicle_telemetry/"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/vehicle_telemetry/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze

# COMMAND ----------

bronze_df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze count: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Enforcement & Type Casting

# COMMAND ----------

typed_df = (
    bronze_df
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    .withColumn("speed_kmh",        col("speed_kmh").cast(DoubleType()))
    .withColumn("engine_temp_c",    col("engine_temp_c").cast(DoubleType()))
    .withColumn("fuel_level_pct",   col("fuel_level_pct").cast(DoubleType()))
    .withColumn("rpm",              col("rpm").cast(IntegerType()))
    .withColumn("brake_pressure_bar", col("brake_pressure_bar").cast(DoubleType()))
    .withColumn("vehicle_id",       upper(trim(col("vehicle_id"))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null & Range Validation

# COMMAND ----------

validated_df = (
    typed_df
    # Drop rows missing critical keys
    .filter(col("event_id").isNotNull())
    .filter(col("vehicle_id").isNotNull())
    .filter(col("event_timestamp").isNotNull())
    # Clamp physically impossible values
    .withColumn("speed_kmh",
        when((col("speed_kmh") < 0) | (col("speed_kmh") > 250), None)
        .otherwise(col("speed_kmh")))
    .withColumn("engine_temp_c",
        when((col("engine_temp_c") < 40) | (col("engine_temp_c") > 150), None)
        .otherwise(col("engine_temp_c")))
    .withColumn("fuel_level_pct",
        when((col("fuel_level_pct") < 0) | (col("fuel_level_pct") > 100), None)
        .otherwise(col("fuel_level_pct")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication
# MAGIC Keep the latest record per (vehicle_id, event_timestamp)

# COMMAND ----------

window_spec = Window.partitionBy("vehicle_id", "event_timestamp").orderBy(col("_ingested_at").desc())

dedup_df = (
    validated_df
    .withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

print(f"After dedup: {dedup_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Flagging

# COMMAND ----------

flagged_df = (
    dedup_df
    .withColumn("flag_overspeed",   when(col("speed_kmh") > 120, lit(1)).otherwise(lit(0)))
    .withColumn("flag_overheat",    when(col("engine_temp_c") > 110, lit(1)).otherwise(lit(0)))
    .withColumn("flag_harsh_brake", when(col("brake_pressure_bar") > 65, lit(1)).otherwise(lit(0)))
    .withColumn("flag_low_fuel",    when(col("fuel_level_pct") < 15, lit(1)).otherwise(lit(0)))
    .withColumn("event_date",       col("event_timestamp").cast("date"))
    .withColumn("_silver_processed_at", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Delta Table

# COMMAND ----------

(
    flagged_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date", "vehicle_id")
    .save(SILVER_PATH)
)

print("Silver write complete.")
flagged_df.select("vehicle_id", "event_timestamp", "speed_kmh", "engine_temp_c",
                  "flag_overspeed", "flag_overheat").show(10, truncate=False)
