# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Drivers.json File ##

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using the Spark Dataframe API ####

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), False),
                                 StructField("surname", StringType(), True)
                       ])
              

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                     StructField("driverRef", StringType(), True),
                     StructField("number", IntegerType(), True),
                     StructField("code", StringType(), True),
                     StructField("name", name_schema),               
                     StructField("dob", DateType(), True),
                     StructField("nationality", StringType(), True),
                     StructField("url", StringType(), True)
                    ])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json("/mnt/formula1/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop the URL Column ####

# COMMAND ----------

drivers_dropped_df = drivers_df.drop('url')

# COMMAND ----------

display(drivers_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date ####

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

drivers_renamed_df = drivers_dropped_df.withColumnRenamed("driverId", "driver_id") \
                                        .withColumnRenamed("driverRef", "driver_ref") \
                                        .withColumn("ingestion_date", current_timestamp()) \
                                        .withColumn('data_source', lit(data_source))

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Transformed Name ####

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

# COMMAND ----------

drivers_final_df = drivers_renamed_df.withColumn("name", concat(col('name.forename'), lit(" "), col('name.surname')))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to the parquet file ####

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1/transformed/drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1/transformed/drivers

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1/transformed/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")
