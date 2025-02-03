# Databricks notebook source
# MAGIC %md
# MAGIC #Access Data Lake using Cluster Scope Credentials 
# MAGIC 1. set the spark config for fs.azure.account.key in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@azformula1dlsa.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@azformula1dlsa.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


