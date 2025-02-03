# Databricks notebook source
# MAGIC %md
# MAGIC #Access Data Lake using SAS Token
# MAGIC 1. set the spark config for sas token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

formula1dl_demo_sas_token = dbutils.secrets.get(scope = "formula1-scope", key = "azformula1dl-demo1-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.azformula1dlsa.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.azformula1dlsa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.azformula1dlsa.dfs.core.windows.net", formula1dl_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@azformula1dlsa.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@azformula1dlsa.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


