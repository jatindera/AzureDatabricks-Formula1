# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Dataframes using SQL
# MAGIC 1. Create temporary views on dataframes
# MAGIC 1. Access the view from SQL cell
# MAGIC 1. Access the view from python cell

# COMMAND ----------

# MAGIC %run
# MAGIC "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{curated_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results where race_year=2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * from v_race_results WHERE race_year={p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temporary View ###

# COMMAND ----------

race_results_df.createOrReplaceTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOWS TABLES in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------

spark.sql("select * from global_temp.gv_race_results").show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


