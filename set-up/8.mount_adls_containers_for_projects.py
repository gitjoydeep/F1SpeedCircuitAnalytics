# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake containers for the project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # get secret key from key valut
    client_id = dbutils.secrets.get(scope = "formula1-scope", key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = 'formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = "formula1-scope", key = 'formula1-app-client-secret')
    # set spark configuraion 
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
        # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

# mount the storage account containers 
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('azformula1dlsa', 'demo')

# COMMAND ----------

mount_adls('azformula1dlsa', 'raw')

# COMMAND ----------

display(dbutils.fs.ls("/mnt/azformula1dlsa/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/azformula1dlsa/demo/circuits.csv"))
