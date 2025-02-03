# Databricks notebook source
# MAGIC %md
# MAGIC ##Explore the cabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope ="azformula1-scope")
dbutils.secrets.list(scope ="formula1-scope")

# COMMAND ----------


