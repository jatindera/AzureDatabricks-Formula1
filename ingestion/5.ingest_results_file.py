# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest results.json File ##

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using the Spark Dataframe API ####

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                     StructField("raceId", IntegerType(), True),
                     StructField("driverId", IntegerType(), True),
                     StructField("constructorId", IntegerType(), True),
                     StructField("number",  IntegerType(), True),               
                     StructField("grid",  IntegerType(), True),
                     StructField("position", IntegerType(), True),
                     StructField("positionText", StringType(), True),
                     StructField("positionOrder",  IntegerType(), True),
                     StructField("points",  FloatType(), True),
                     StructField("laps",  IntegerType(), True),
                     StructField("time",  StringType(), True),
                     StructField("milliseconds",  IntegerType(), True),
                     StructField("fastestLap",  IntegerType(), True),
                     StructField("rank",  IntegerType(), True),
                     StructField("fastestLapTime",  StringType(), True),
                     StructField("fastestLapSpeed",  FloatType(), True),
                     StructField("statusId",  IntegerType(), True),
                    
                    ])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json("/mnt/formula1/raw/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add ingestion date ####

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
                                              .withColumnRenamed("raceId", "race_id") \
                                              .withColumnRenamed("driverId", "driver_id") \
                                              .withColumnRenamed("constructorId", "constructor_id") \
                                              .withColumnRenamed("positionText", "position_text") \
                                              .withColumnRenamed("positionOrder", "position_order") \
                                              .withColumnRenamed("fastestLap", "fastest_lap") \
                                              .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                              .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                              .withColumn("ingestion_date", current_timestamp()) \
                                              .withColumn('data_source', lit(data_source))

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop StatusId ####

# COMMAND ----------

results_final_df = results_renamed_df.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to the parquet file ####

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/formula1/transformed/results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1/transformed/results

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1/transformed/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")
