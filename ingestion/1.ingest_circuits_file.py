# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 1.Read the csv file using the Spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("prm_data_source", "")
var_data_source = dbutils.widgets.get("prm_data_source")

# COMMAND ----------

dbutils.widgets.text("prm_file_date", "")
var_file_date = dbutils.widgets.get("prm_file_date")

# COMMAND ----------

var_file_date

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df=spark.read \
    .option("header", "true") \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/{var_file_date}/circuits.csv")
    

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.Select only the required column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df=circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.Rename the column as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_rename_df = (
    circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "Latitude") \
    .withColumnRenamed("lng", "Longitude") \
    .withColumnRenamed("alt", "Altitude") \
    .withColumn("data_source", lit(var_data_source)) \
    .withColumn("file_date", lit(var_file_date))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Add ingetion date to the dataFrame

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_rename_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.Write data to datalake in parquet format 

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE f1_processed.circuits;
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("success")
