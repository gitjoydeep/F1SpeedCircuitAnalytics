# Databricks notebook source
# MAGIC %md
# MAGIC ####1.Ingest driver.json file

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(
    fields=[
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
        .json(f"{raw_folder_path}/{var_file_date}/drivers.json"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2.Rename columns and add new columns
# MAGIC 1.driverId rename to driver_id \
# MAGIC 2.DriverRef rename to driver_ref \
# MAGIC 3.Ingestion date added \
# MAGIC 4.name added with concatenations of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

drivers_with_column_df = (
    add_ingestion_date(drivers_df)
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("driverRef", "driver_ref")
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
    .withColumn("file_date", lit(var_file_date))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.Drop unwanted column
# MAGIC 1.name.forname \
# MAGIC 2.name.surname \
# MAGIC 3. url

# COMMAND ----------

drivers_final_df =drivers_with_column_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####4.Write the outpout data into processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE f1_processed.drivers;
# MAGIC select * from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("success")
