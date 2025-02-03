# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC ## Explore DBFS root
# MAGIC     1.list all the folders in the dbfs root
# MAGIC     2.Interect with DBFS file Browse
# MAGIC     3.Upload files into DBFS root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/circuits.csv'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------


