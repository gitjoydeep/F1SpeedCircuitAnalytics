# Databricks notebook source
# MAGIC %md
# MAGIC ###### Read all the data as required

# COMMAND ----------

dbutils.widgets.text("prm_file_date", "")
var_file_date = dbutils.widgets.get("prm_file_date")

# COMMAND ----------

display(var_file_date)

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/common_functions"

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")


# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team_name")


# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location")


# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_timestamp") \
    .withColumnRenamed("date", "race_date")


# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{var_file_date}'") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date") \
        


# COMMAND ----------

# MAGIC %md
# MAGIC ###Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)


# COMMAND ----------

race_results_df =  results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id, "inner") \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = add_ingestion_date(race_results_df).select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team_name", "grid", "fastest_lap", "race_time", "points" , "position", "result_file_date" ) \
.withColumn("create_datetime", current_timestamp()) \
.withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

#overwrite_partition(final_df, "f1_presentation", "race_results", "race_id")

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table f1_presentation.race_results 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id, count(1) from f1_presentation.race_results
# MAGIC group by race_id
# MAGIC order by race_id desc
# MAGIC

# COMMAND ----------

#the above code is equivalent to the following. We created a functions to make it more readable
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

dbutils.notebook.exit("success")
