# Databricks notebook source
# MAGIC %run
# MAGIC "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{transformed_folder_path}/circuits") \
.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{transformed_folder_path}/races").filter("race_year=2019") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner Join ##

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df.select(races_circuits_df.circuit_name, races_circuits_df.race_name).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lef Outer Join ##

# COMMAND ----------

# MAGIC %md
# MAGIC #### All records in left DF stays. If condition does not match, right DF will show null ####

# COMMAND ----------

# Test left out join. Put filter condition to remove few records
circuits_df = spark.read.parquet(f"{transformed_folder_path}/circuits") \
.filter("circuit_id<70") \
.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{transformed_folder_path}/races").filter("race_year=2019") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Right Outer Join ##

# COMMAND ----------

# MAGIC %md
# MAGIC #### All records in right DF stays. If condition does not match, left DF will show null ####

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Outer Join ##

# COMMAND ----------

# Full outer join
races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi Joins - Can't specify any column from the right. Thats why it is good to use inner join with select###

# COMMAND ----------

# Following will give error
races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Only columns from left DF. Nothing from right DF #####

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti Joins - Opposite of Semi (Everything on left DF which is not found on the right DF) ###

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross Joins - Notice each circuit id on the left is being joined to every record on the right DF###
# MAGIC - Can be used to duplicate the records

# COMMAND ----------

races_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df.count()

# COMMAND ----------

int(races_df.count()) * int(circuits_df.count())

# COMMAND ----------


