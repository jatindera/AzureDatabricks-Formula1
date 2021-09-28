# Databricks notebook source
# MAGIC %run
# MAGIC "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregation function demo ###

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Built in aggregate functions #####

# COMMAND ----------

races_results_df = spark.read.parquet(f"{curated_folder_path}/race_results")

# COMMAND ----------

demo_df = races_results_df.filter("race_year=2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name= 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name= 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
.withColumnRenamed("sum(points)", "total_points") \
.withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
.show()

# COMMAND ----------

demo_df.groupBy("driver_name") \
.agg(sum("points"), countDistinct("race_name"))  \
.withColumnRenamed("sum(points)", "total_points") \
.withColumnRenamed("count(race_name)", "number_of_races") \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windows Functions ###

# COMMAND ----------

demo_df = races_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year","driver_name") \
.agg(sum("points"), countDistinct("race_name"))  \
.withColumnRenamed("sum(points)", "total_points") \
.withColumnRenamed("count(race_name)", "number_of_races")

# COMMAND ----------

display(demo_grouped_df)


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))

# COMMAND ----------

demo_grouped_df.withColumn("rank", rank().over(driver_rank_spec)).show(100)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


