# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run
# MAGIC "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC - Wild card support to read multiple files

# COMMAND ----------

# lap_times_df = spark.read \
# 2
# .schema(lap_times_schema) \
# 3
# .csv("/mnt/formula1dl/raw/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC - Read all files from a folder

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn('data_source', lit(data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{transformed_folder_path}/lap_times")

# COMMAND ----------

display(spark.read.parquet(f"{transformed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")
