# Databricks notebook source
# %md
## Where and filter are synonymous. You can use either ##

# COMMAND ----------

raw_folder_path = '/mnt/formula1/raw'
transformed_folder_path = '/mnt/formula1/transformed'

# COMMAND ----------

# races_df = spark.read.parquet(f"{transformed_folder_path}/races")

# COMMAND ----------

# display(races_df)

# COMMAND ----------

# %md
##### SQl Style #####

# COMMAND ----------

# races_filtered_df = races_df.filter("race_year=2019  and round <=5")

# COMMAND ----------

# display(races_filtered_df)

# COMMAND ----------

# %md
##### Python Style #####

# COMMAND ----------

# races_filtered_df = races_df.filter((races_df["race_year"]==2019) & (races_df["round"]<=5)
