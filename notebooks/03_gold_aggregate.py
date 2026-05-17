# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Gold Layer: Aggregated Fleet Analytics
# MAGIC
# MAGIC Reads Silver Delta table and produces fleet-level daily KPIs:
# MAGIC - Per-vehicle daily metrics (avg speed, max temp, trip count)
# MAGIC - Anomaly summary counts
# MAGIC - Fleet health score
# MAGIC
# MAGIC Uses Delta MERGE for idempotent incremental updates.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, min, sum, count, round as spark_round,
    current_timestamp, lit
)
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("VehicleTelemetry_Gold").getOrCreate()

# COMMAND ----------

STORAGE_ACCOUNT = "yourstorageaccount"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/vehicle_telemetry/"
GOLD_PATH   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/vehicle_daily_metrics/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate: Vehicle Daily KPIs

# COMMAND ----------

gold_df = (
    silver_df
    .groupBy("vehicle_id", "event_date")
    .agg(
        count("event_id").alias("total_events"),
        spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
        spark_round(max("speed_kmh"), 2).alias("max_speed_kmh"),
        spark_round(avg("engine_temp_c"), 2).alias("avg_engine_temp_c"),
        spark_round(max("engine_temp_c"), 2).alias("max_engine_temp_c"),
        spark_round(avg("fuel_level_pct"), 2).alias("avg_fuel_level_pct"),
        spark_round(min("fuel_level_pct"), 2).alias("min_fuel_level_pct"),
        sum("flag_overspeed").alias("overspeed_events"),
        sum("flag_overheat").alias("overheat_events"),
        sum("flag_harsh_brake").alias("harsh_brake_events"),
        sum("flag_low_fuel").alias("low_fuel_events"),
    )
    # Fleet health score: 100 minus weighted penalty for anomalies
    .withColumn("health_score",
        spark_round(
            lit(100)
            - (col("overspeed_events") * lit(2))
            - (col("overheat_events") * lit(5))
            - (col("harsh_brake_events") * lit(3))
            - (col("low_fuel_events") * lit(1)),
            1
        )
    )
    .withColumn("_gold_updated_at", current_timestamp())
)

print(f"Gold records: {gold_df.count()}")
gold_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta MERGE (Upsert) – Idempotent Write

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, GOLD_PATH):
    gold_table = DeltaTable.forPath(spark, GOLD_PATH)
    (
        gold_table.alias("target")
        .merge(
            gold_df.alias("source"),
            "target.vehicle_id = source.vehicle_id AND target.event_date = source.event_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Delta MERGE complete.")
else:
    gold_df.write.format("delta").mode("overwrite").partitionBy("event_date").save(GOLD_PATH)
    print("Initial Gold write complete.")
