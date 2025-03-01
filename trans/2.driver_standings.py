# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce Driver Standings

# COMMAND ----------

dbutils.widgets.text("prm_file_date", "")
var_file_date = dbutils.widgets.get("prm_file_date")    

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Find race years for which data is to be reprocessed 

# COMMAND ----------

race_results_list = spark.read.format("delta").load(
    f"{presentation_folder_path}/race_results"
).filter(f"file_date = '{var_file_date}'")


# COMMAND ----------

race_years_list = df_column_to_list(race_results_list, "race_year")


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col('race_year').isin(race_years_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col
driver_standings_df = race_results_df \
    .groupBy("race_year" , "driver_name" , "driver_nationality") \
    .agg(sum("points").alias("total_points") ,
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#overwrite_partition(final_df, "f1_presentation", "driver_standings", "race_year")

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table f1_presentation.driver_standings;
# MAGIC select distinct race_year from f1_presentation.driver_standings 
# MAGIC order by race_year desc;

# COMMAND ----------

#**** the above function is having the sam write mode into the drier_standings table***
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

dbutils.notebook.exit("success")
