-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Managed Database ##

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1/discovery"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

DESC DATABASE extended f1_processed;

-- COMMAND ----------


