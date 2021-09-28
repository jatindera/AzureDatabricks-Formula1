# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('secretscope-pigolu-kv')

# COMMAND ----------

dbutils.secrets.get(scope="secretscope-pigolu-kv", key="sp-pigolu-formula1-clientid")

# COMMAND ----------

# Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks with Service Principal and OAuth
# Author: Jatinder Arora
def mountADLS2(adlsContainerName, adlsFolderName):
  adlsAccountName = "adls2pigolu"
  secretScopeName = "secretscope-pigolu-kv"
  adlsFolderName = adlsFolderName.lower()
  mountPoint = f'/mnt/{adlsContainerName}/{adlsFolderName}'
  # Application (Client) ID
  applicationId = dbutils.secrets.get(scope=secretScopeName,key="sp-pigolu-formula1-clientid")

  # Application (Client) Secret Key
  authenticationKey = dbutils.secrets.get(scope=secretScopeName,key="sp-pigolu-formula1-secret")

  # Directory (Tenant) ID
  tenandId = dbutils.secrets.get(scope=secretScopeName,key="tenantid")

  endpoint = f"https://login.microsoftonline.com/{tenandId}/oauth2/token"
  adlsFolderName = adlsFolderName.upper()
  source = f"abfss://{adlsContainerName}@{adlsAccountName}.dfs.core.windows.net/{adlsFolderName}"

  # Connecting using Service Principal secrets and OAuth
  configs = {"fs.azure.account.auth.type": "OAuth",
             "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
             "fs.azure.account.oauth2.client.id": applicationId,
             "fs.azure.account.oauth2.client.secret": authenticationKey,
             "fs.azure.account.oauth2.client.endpoint": endpoint}
  # Mounting ADLS Storage to DBFS
  # Mount only if the directory is not already mounted
  if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
      source = source,
      mount_point = mountPoint,
      extra_configs = configs)

# COMMAND ----------

#mount formula1/raw
mountADLS2('formula1', 'RAW')

# COMMAND ----------

#mount formula1/processed
mountADLS2('formula1', 'DISCOVERY')

# COMMAND ----------

mountADLS2('formula1', 'CURATED')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls('/mnt/formula1/raw')

# COMMAND ----------

dbutils.fs.ls('/mnt/formula1/discovery')

# COMMAND ----------

dbutils.fs.ls('/mnt/formula1/curated')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Formula1 data is available at following URL for learning purpose ##
# MAGIC #### http://ergast.com/mrd/ ####

# COMMAND ----------

# unmount exmample
# dbutils.fs.unmount('/mnt/formula1/raw')
dbutils.fs.unmount('/mnt/formula1/processed')
# dbutils.fs.unmount('/mnt/formula1/transformed')

# COMMAND ----------

# 
