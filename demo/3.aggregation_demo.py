# Databricks notebook source
# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/F1SpeedCircuitAnalytics/Includes/common_functions"

# COMMAND ----------

display('/mnt/azformula1dlsa/presentation/race_results')

# COMMAND ----------

race_results_df = spark.read.parquet(f"/{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results_df.filter(race_results_df.race_year == 2020)

# COMMAND ----------

from pyspark.sql.functions import count, avg, max, min, sum, countDistinct

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name == 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
    .withColumnRenamed("sum(points)", "total_points") \
    .withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
    .show()

# COMMAND ----------

demo_df \
    .groupBy("driver_name") \
        .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
        .show()

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped_df = demo_df \
    .groupBy("race_year", "driver_name") \
        .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

from pyspark.sql.functions import desc

# COMMAND ----------

display(demo_grouped_df.orderBy("race_year", desc("total_points")))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("driver_rank", rank().over(driverRankSpec)).show()

# COMMAND ----------


