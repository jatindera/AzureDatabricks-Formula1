# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE database demo

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database extended demo

# COMMAND ----------

# MAGIC %sql
# MAGIC select CURRENT_DATABASE();

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select CURRENT_DATABASE();

# COMMAND ----------

# MAGIC %run
# MAGIC "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{curated_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save as table in the demo database ###

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Below is managed table #####

# COMMAND ----------

race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended demo.race_results_python

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.race_results_python where race_year=2020

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create table using SQL ###

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists race_results_sql
# MAGIC as
# MAGIC select * from demo.race_results_python where race_year=2020

# COMMAND ----------

# MAGIC %sql
# MAGIC select CURRENT_DATABASE();

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended demo.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table demo.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## External Tables and effects of dropping ##

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using Python ####

# COMMAND ----------

race_results_df.write.format("parquet").option("path", f"{curated_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc demo.race_results_ext_py

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using SQL ####

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE demo.race_results_ext_sql
# MAGIC (race_year INT,
# MAGIC race_name STRING,
# MAGIC race_date TIMESTAMP,
# MAGIC circuit_location STRING,
# MAGIC driver_name STRING,
# MAGIC driver_number INT,
# MAGIC driver_nationality STRING,
# MAGIC team STRING,
# MAGIC grid INT,
# MAGIC fastest_lap INT,
# MAGIC race_time STRING,
# MAGIC points FLOAT,
# MAGIC position INT,
# MAGIC created_date TIMESTAMP
# MAGIC )
# MAGIC USING parquet
# MAGIC LOCATION "/mnt/formula1/curated/race_results_ext_sql"

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo.race_results_ext_sql
# MAGIC select * from demo.race_results_ext_py where race_year=2020

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table race_results_ext_py

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views on Tables ##

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view v_result_races
# MAGIC as
# MAGIC select * from demo.race_results_python
# MAGIC where race_year=2020

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_result_races

# COMMAND ----------

# MAGIC %md
# MAGIC #### Following is global though temporary. Cluster shutdown will remove this view ####

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view gv_result_races
# MAGIC as
# MAGIC select * from demo.race_results_python
# MAGIC where race_year=2012

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use global temp to access the global view ####

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_result_races

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %md
# MAGIC #### Following is permanent view. This will go under default database if you don't put the db name ####

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view demo.pv_result_races
# MAGIC as
# MAGIC select * from demo.race_results_python
# MAGIC where race_year=2012

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------



# COMMAND ----------


