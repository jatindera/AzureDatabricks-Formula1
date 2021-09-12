# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file ###

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step1 - Read the CSV file using the Spark dataframe reader #####

# COMMAND ----------

df = spark.read.csv('/mnt/formaula1/RAW')
