# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file ###

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run 
# MAGIC "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read the CSV file using the Spark dataframe reader ####

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1/raw

# COMMAND ----------

# MAGIC %md
# MAGIC #### inferSchema is slow and not recommended for production ####

# COMMAND ----------

# circuits_df = spark.read \ 
# .option("header", True) \ 
# .option('inferSchema', True) \ 
# .csv('dbfs:/mnt/formaula1/raw/circuits.csv')

# COMMAND ----------

# circuits_df.show()
# display(circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - User defined schema ###

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuites_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                     StructField("circuitRef", StringType(), True),
                     StructField("name", StringType(), True),
                     StructField("location", StringType(), True),
                     StructField("country", StringType(), True),
                     StructField("lat", DoubleType(), True),
                     StructField("lng", DoubleType(), True),
                     StructField("alt", IntegerType(), True),
                     StructField("url", StringType(), True)
                    ])

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuites_schema).csv(f'{raw_folder_path}/circuits.csv')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Print schema ####

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe()

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop the URL column ####
# MAGIC * user either drop or select

# COMMAND ----------

# circuit_df = circuit_df.drop('url').collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3 - Select Statement ###
# MAGIC * circuit_df = circuit_df.select('circuitId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Above statement allows you to just select columns. With following, you can use the column functions as well. For example: You would like to change the name of a column. ####

# COMMAND ----------

# MAGIC %md
# MAGIC ## There are multiple ways to achieve above select ##
# MAGIC * circuit_df = circuit_df.select(circuit_df.circuitId, circuit_df.circuitRef, circuit_df.name', circuit_df.location, circuit_df.country, circuit_df.lat, circuit_df.lng, circuit_df.alt)
# MAGIC * circuit_df = circuit_df.select(circuit_df['circuitId'], circuit_df['circuitRef'], circuit_df['name'], circuit_df['location'], circuit_df['country'], circuit_df['lat'], circuit_df['lng'], circuit_df['alt'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other way to achieve above is using col ##
# MAGIC * from pyspark.sql.functions import col
# MAGIC * circuit_df = circuit_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt')

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Rename the columns using withColumnRenamed ###
# MAGIC * Other Way
# MAGIC   * circuit_df = circuit_df.select(col('circuitId').alias('circuit_id'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
.withColumnRenamed('circuitRef', 'circuit_ref') \
.withColumnRenamed('lat', 'latitude') \
.withColumnRenamed('lng', 'longitude') \
.withColumnRenamed('alt', 'altitude') \
.withColumn('data_source', lit(data_source))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Add ingestion Date ##

# COMMAND ----------

# MAGIC %md
# MAGIC #### current_timestamp is of type col so same date is added against every row. To add a column with string value you need to use lit ####

# COMMAND ----------

#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
# withColumn("env", lit("production"))

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Write data as Parquet ###

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("f{transformed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1/transformed/circuits

# COMMAND ----------

df = spark.read.parquet(f"{transformed_folder_path}/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
