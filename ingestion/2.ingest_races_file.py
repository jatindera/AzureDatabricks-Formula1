# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file ###

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step1 - Read the CSV file using the Spark dataframe reader ##

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                     StructField("year", IntegerType(), True),
                     StructField("round", IntegerType(), True),
                     StructField("circuitId", IntegerType(), True),
                     StructField("name", StringType(), True),
                     StructField("date", DateType(), True),
                     StructField("time", StringType(), True),
                     StructField("url", StringType(), True)
                    ])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv('dbfs:/mnt/formula1/raw/races.csv')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Print schema ####

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3  - Add ingestion Date and race timestamp ##

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                 .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss' ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 - Drop the URL column using Select Statement ###

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId'), col('year'), col('round'), col('circuitId'), col('name'), col('race_timestamp'))

# COMMAND ----------

# display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Rename the columns using withColumnRenamed ##

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('year', 'race_year') \
.withColumnRenamed('circuitId', 'circuit_id') \
.withColumn('data_source', lit(data_source))

# COMMAND ----------

# display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Add ingestion Date ##

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Write data as Parquet ##

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/formula1/transformed/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1/transformed/races

# COMMAND ----------

df = spark.read.parquet("/mnt/formula1/transformed/races")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


