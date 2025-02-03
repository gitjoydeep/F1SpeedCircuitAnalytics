# Databricks notebook source
# MAGIC %md
# MAGIC #### Access dataFrame using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Objectives 
# MAGIC 1. Temporary views on dataFrame
# MAGIC 2. Access the View from SQL Cell
# MAGIC 3. Access the view from Python Cell

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.parquet(f"/{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results
# MAGIC where race_year = 2020 and position is not null
# MAGIC order by position

# COMMAND ----------

race_results_2019_df =  spark.sql(f"select * from v_race_results \
where race_year = 2019 and position is not null \
order by position")

# COMMAND ----------

display(race_results_2019_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temporary View 
# MAGIC 1. Create Global Temporary views on dataFrames
# MAGIC 2. Access the View from SQL Cell
# MAGIC 3. Access the view from Python Cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

 spark.sql(f"select * from global_temp.gv_race_results").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------


