# Databricks notebook source
# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Lesson Objectives 
# MAGIC 1. Spark SQL documentation
# MAGIC 2. Create database demo
# MAGIC 3. Data tab in the UI
# MAGIC 4. SHOW command
# MAGIC 5. DESCRIBE command
# MAGIC 6. FInd the current database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC use database demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE demo

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED demo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Learning Objectives
# MAGIC 1. Create manage Table using python
# MAGIC 2. Create manage Table using SQL
# MAGIC 3. Effect of dropping a managed table
# MAGIC 4. Describe table

# COMMAND ----------

race_results_df = spark.read.parquet(f"/{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

# COMMAND ----------

# MAGIC %sql
# MAGIC use database demo;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from demo.race_results_python 
# MAGIC where race_year = 2020

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc extended  race_results_python;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE demo.race_results_sql
# MAGIC As
# MAGIC select * from demo.race_results_python 
# MAGIC where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.race_results_sql

# COMMAND ----------

# MAGIC %md
# MAGIC ###Learning Objectives
# MAGIC 1. Create external Table using python
# MAGIC 2. Create external Table using SQL
# MAGIC 3. Effect of dropping a external  table
# MAGIC 4. Describe  external tables table

# COMMAND ----------

race_results_df.write.format("parquet").option("path", f"/{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended demo.race_results_ext_py

# COMMAND ----------


