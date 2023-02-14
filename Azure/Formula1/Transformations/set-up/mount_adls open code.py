# Databricks notebook source
storage_account_name = "formula1dl2323"
client_id            = "a4c3b3c6-1534-40fc-90d7-d24ef59ef5ec"
tenant_id            = "8523adb9-76df-4de3-8868-c1d409243fbc"
client_secret        = "aXt8Q~AtIMRSSbrY9rlZcBhZk5SB6uk6to07DbVP"

# COMMAND ----------

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

# COMMAND ----------

container_name = "raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl2323/raw")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl2323/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl2323/raw")
