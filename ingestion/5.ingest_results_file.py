# Databricks notebook source
# MAGIC %md
# MAGIC ###1- Read result.json file using spark dataFrame reader API

# COMMAND ----------

dbutils.widgets.text("prm_data_source", "")
var_data_source = dbutils.widgets.get("prm_data_source")

# COMMAND ----------

dbutils.widgets.text("prm_file_date", "")
var_file_date = dbutils.widgets.get("prm_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType 

# COMMAND ----------

results_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

result_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{var_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

result_with_columns_df = (
    add_ingestion_date(result_df)
    .withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionText", "position_text")
    .withColumnRenamed("positionOrder", "position_order")
    .withColumnRenamed("fastestLap", "fastest_lap")
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
    .withColumn("file_date", lit(var_file_date))
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

result_final_df=result_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### De-dupe the dataFrame

# COMMAND ----------

results_deduped_df = result_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Write the output to the processed container

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method -1 Incremental Load

# COMMAND ----------

# for race_id_list in result_final_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id={race_id_list['race_id']})")

# COMMAND ----------

# result_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method -2 Incremental Load

# COMMAND ----------

# This code has been moved to include folder
# def re_arrange_partition_columns(input_df, partition_column):
#     column_list = []
#     for column_name in input_df.schema.names:
#         if column_name not in [partition_column, "file_date", "ingestion_date"]:
#             column_list.append(column_name)
#     column_list.append("file_date")
#     column_list.append("ingestion_date")
#     column_list.append(partition_column)
#     output_df = input_df.select(column_list)
#     return output_df

# COMMAND ----------

# invoking this function is just to test the results
# output_df = re_arrange_partition_columns(result_final_df, "race_id")

# COMMAND ----------

# # This code has been moved to include folder
# def overwrite_partition(input_df, db_name, table_name, partition_column):
#     output_df = re_arrange_partition_columns(input_df, partition_column)

#     spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

#     if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
#         output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#     else:
#         output_df.write.mode("overwrite").partitionBy(partition_column).format(
#             "parquet"
#         ).saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

#overwrite_partition(result_final_df, "f1_processed", "results", "race_id")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# This code is cloubed inside overrwrite_partition function
#  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# result_final_df = result_final_df.select(
#     "result_id",
#     "driver_id",
#     "constructor_id",
#     "number",
#     "grid",
#     "position",
#     "position_text",
#     "position_order",
#     "points",
#     "laps",
#     "time",
#     "milliseconds",
#     "fastest_lap",
#     "rank",
#     "fastest_lap_time",
#     "fastest_lap_speed",
#     "file_date",
#     "ingestion_date",
#     "race_id",
# )

# COMMAND ----------

# # This code is cloubed inside overrwrite_partition function
# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     result_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     result_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table f1_processed.results;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select  race_id, driver_id, count(*) 
# MAGIC from f1_processed.results 
# MAGIC group by race_id, driver_id 
# MAGIC having count(*) > 1
# MAGIC order by race_id desc

# COMMAND ----------

display(spark.read.format("delta").load(f"{processed_folder_path}/results").count())

# COMMAND ----------

dbutils.notebook.exit("success")
