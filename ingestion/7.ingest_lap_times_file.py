# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest laptime.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read the csv files using spark dataFrame reader API

# COMMAND ----------

dbutils.widgets.text("prm_data_source", "")
var_data_source = dbutils.widgets.get("prm_data_source")

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

lap_times_schema=StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("lap", IntegerType(), True),
                                 StructField("position", IntegerType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True),
])
                          

# COMMAND ----------

lap_times_df = (
    spark.read.schema(lap_times_schema) 
    .csv(f"{raw_folder_path}/{var_file_date}/lap_times/lap_times_split*.csv")
)

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Rename column name and add new columns
# MAGIC  - Rename driverId and raceId
# MAGIC  - Add ingetion date with currecnt timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_df) \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumn("file_date", lit(var_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Write the output to the processed container

# COMMAND ----------

#overwrite_partition(lap_times_final_df, "f1_processed", "lap_times", "race_id")

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table f1_processed.lap_times;
# MAGIC select  race_id, count(*) from f1_processed.lap_times group by race_id order by race_id desc

# COMMAND ----------

#the avobe code is equivalent to the following
# lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

dbutils.notebook.exit("success")
