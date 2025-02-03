# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Races.csv file

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

# MAGIC %md
# MAGIC ##### 1.Read the csv file using the spark dataFrame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

race_schema=StructType(fields=[StructField("raceId", IntegerType(), False),
                               StructField("year", IntegerType(), True),
                               StructField("round", IntegerType(), True),
                               StructField("circuitId", IntegerType(), True),
                               StructField("name", StringType(), True),
                               StructField("date", DateType(), True),
                               StructField("time", StringType(), True),    
                               StructField("url", StringType(), True)
])

# COMMAND ----------

race_df=spark.read \
    .options(header="true") \
    .schema(race_schema) \
    .csv (f"{raw_folder_path}/{var_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.Add ingestion date and race timestamp to the dataFrame

# COMMAND ----------

from pyspark.sql.functions import  to_timestamp, concat, col, lit

# COMMAND ----------

race_with_timestamp_df = add_ingestion_date(race_df) \
                                .withColumn("race_timestamp", to_timestamp(
                                    concat(col("date"), lit(" "), col("time")), 
                                    "yyyy-MM-dd HH:mm:ss"
                                ))\
                                .withColumn("file_date", lit(var_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. select only column required and renamed

# COMMAND ----------

races_selected_df = race_with_timestamp_df.select(
    col("raceId").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    col("date"),
    col("time"),
    col("ingestion_date"),
    col("race_timestamp"),
    col("file_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. write the output into processed container 

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE f1_processed.races;
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("success")
