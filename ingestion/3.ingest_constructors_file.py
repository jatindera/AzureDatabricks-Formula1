# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Constructors.json File ##

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using the Spark Dataframe API ####

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING,  name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json("/mnt/formula1/raw/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop the URL Column ####

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date ####

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "contructor_id") \
                                              .withColumnRenamed("constructorRef", "constructor_ref") \
                                              .withColumn("ingestion_date", current_timestamp()) \
                                              .withColumn('data_source', lit(data_source))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data to the parquet file ####

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1/transformed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1/transformed/constructors

# COMMAND ----------

dbutils.notebook.exit("Success")
