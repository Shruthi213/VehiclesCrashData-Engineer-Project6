# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id":"",
"fs.azure.account.oauth2.client.secret":'',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com//oauth2/token"
}

dbutils.fs.mount(
source = "abfss://raw@vehiclesstoragedata123.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/Raw",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/Raw/"

# COMMAND ----------

display(dbutils.fs.ls("/mnt/Raw/vehiclesdata/"))
