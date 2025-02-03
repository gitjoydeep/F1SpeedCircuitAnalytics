# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest pitstop.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read the pitstops.json file using spark dataFrame reader API

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

pit_stop_schema=StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("stop", IntegerType(), True),
                                 StructField("lap", IntegerType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("duration", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True),
])
                          

# COMMAND ----------

pit_stop_df = (
    spark.read.schema(pit_stop_schema) 
    .option("multiline", "true")
    .json(f"{raw_folder_path}/{var_file_date}/pit_stops.json")
)

# COMMAND ----------

display(pit_stop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Rename column name and add new columns
# MAGIC  - Rename driverId and raceId
# MAGIC  - Add ingetion date with currecnt timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stop_final_df = add_ingestion_date(pit_stop_df) \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumn("file_date", lit(var_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Write the output to the processed container

# COMMAND ----------

#overwrite_partition(pit_stop_final_df, "f1_processed", "pit_stops", "race_id")
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pit_stop_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC refresh table f1_processed.pit_stops;
# MAGIC select  race_id, count(*) from f1_processed.pit_stops group by race_id order by race_id desc

# COMMAND ----------

# The above code is equivalent to the following
# pit_stop_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("success")
