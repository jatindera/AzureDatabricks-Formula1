# Databricks notebook source
# MAGIC %md
# MAGIC ##### Why ADF is a better choice ? #####
# MAGIC - Please note that we can run notebooks only sequentially. Notebook 2 will execute only after notebook 1
# MAGIC - Even though databricks provides a way to run notebooks concurrently, it is hard way to implement. Please refer to link <a href="https://docs.databricks.com/notebooks/notebook-workflows.html">Documentation</a>
# MAGIC - ADF provides an easy way to achieve concurrent execution of notebooks.
# MAGIC ##### What if you need to run each/few notebooks on a different type of clusters ? ADF is the way to go. #####

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

print(result)

# COMMAND ----------


