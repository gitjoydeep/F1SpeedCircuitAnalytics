# Databricks notebook source
# MAGIC %md
# MAGIC #Access Data Lake using Access Keys
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

azformula1dlsa_account_key = dbutils.secrets.get(scope ='formula1-scope', key ='formula1-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.azformula1dlsa.dfs.core.windows.net", azformula1dlsa_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@azformula1dlsa.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@azformula1dlsa.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


