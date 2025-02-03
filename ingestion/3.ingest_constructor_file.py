# Databricks notebook source
# MAGIC %md
# MAGIC #### 1.Read the JSON file using the spark dataFrame reader 

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

constructors_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df=spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/{var_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Drop unwanted column from the dataFrame

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

constructors_droped_df=constructors_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Rename the column name and Add new column

# COMMAND ----------

constructors_final_df=add_ingestion_date(constructors_droped_df) \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("constructorRef","constructor_ref")\
    .withColumn("file_date", lit(var_file_date))
    

# COMMAND ----------

# MAGIC %md
# MAGIC #####4.write date into parquet format in processed layer

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable ("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE f1_processed.constructors;
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

display(spark.read.format("delta").load(f"{processed_folder_path}/constructors"))

# COMMAND ----------

dbutils.notebook.exit("success")
