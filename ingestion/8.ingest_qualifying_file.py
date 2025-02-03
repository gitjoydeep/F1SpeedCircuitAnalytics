# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest qualifying.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read the qualifying.json file using spark dataFrame reader API

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualify_schema=StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                   StructField("raceId", IntegerType(), True),
                                   StructField("driverId", IntegerType(), True),
                                   StructField("constructorId", IntegerType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("position", IntegerType(), True),
                                   StructField("q1", StringType(), True),
                                   StructField("q2", StringType(), True),
                                   StructField("q3", StringType(), True)
                                 
])
                          

# COMMAND ----------

qualify_df = (
    spark.read.schema(qualify_schema) 
    .option("multiline", "true")
    .json(f"{raw_folder_path}/{var_file_date}/qualifying/qualifying_split*.json")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Rename column name and add new columns
# MAGIC  - Rename qualifyId,raceId, driverId and constructorId
# MAGIC  - Add ingetion date with currecnt timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_final_df = (
    add_ingestion_date(qualify_df)
    .withColumnRenamed("qualifyId", "qualify_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumn("file_date", lit(var_file_date))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Write the output to the processed container

# COMMAND ----------



# COMMAND ----------

#overwrite_partition(qualifying_final_df, "f1_processed", "qualifying", "race_id")

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table f1_processed.qualifying;
# MAGIC select  race_id, count(*) from f1_processed.qualifying group by race_id order by race_id desc

# COMMAND ----------

#the code above is equivalent to the following
# qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("success")
