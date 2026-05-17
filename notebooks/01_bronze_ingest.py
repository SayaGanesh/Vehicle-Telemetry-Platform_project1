# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Bronze Layer: Raw Telemetry Ingestion
# MAGIC
# MAGIC This notebook reads raw vehicle telemetry JSON files from the ADLS Gen2 landing zone
# MAGIC and writes them to the Bronze Delta table with minimal transformation.
# MAGIC No business logic here — full fidelity preservation.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from datetime import datetime

spark = SparkSession.builder.appName("VehicleTelemetry_Bronze").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

STORAGE_ACCOUNT = "yourstorageaccount"
CONTAINER_BRONZE = "bronze"
CONTAINER_RAW    = "landing"
BRONZE_PATH      = f"abfss://{CONTAINER_BRONZE}@{STORAGE_ACCOUNT}.dfs.core.windows.net/vehicle_telemetry/"
RAW_PATH         = f"abfss://{CONTAINER_RAW}@{STORAGE_ACCOUNT}.dfs.core.windows.net/telemetry/"

run_date = datetime.now().strftime("%Y-%m-%d")
print(f"Run date: {run_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw JSON from Landing Zone

# COMMAND ----------

raw_df = (
    spark.read
    .option("multiline", "false")
    .json(RAW_PATH)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_batch_date", lit(run_date))
)

print(f"Records read: {raw_df.count()}")
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Table

# COMMAND ----------

(
    raw_df.write
    .format("delta")
    .mode("append")
    .partitionBy("_batch_date")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

print("Bronze write complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Validation

# COMMAND ----------

bronze_df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Total records in Bronze: {bronze_df.count()}")
bronze_df.select("vehicle_id", "event_timestamp", "speed_kmh", "_ingested_at").show(5, truncate=False)
