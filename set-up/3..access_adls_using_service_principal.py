# Databricks notebook source
# MAGIC %md
# MAGIC #Access Data Lake using service principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate an secret / password for the application
# MAGIC 3. set Spark Config with App / Client Id, Directory / Teannt Id  Secret
# MAGIC 4. Assign Role 'Storage Data Contributor' to the Data Lake
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "formula1-scope", key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = "formula1-scope", key = 'formula1-app-client-secret')



# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.azformula1dlsa.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azformula1dlsa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azformula1dlsa.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.azformula1dlsa.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azformula1dlsa.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@azformula1dlsa.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@azformula1dlsa.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


