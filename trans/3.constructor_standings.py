# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce constructor standings

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

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{var_file_date}'")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_years_list = df_column_to_list(race_results_df, "race_year")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col('race_year').isin(race_years_list))

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col
constructor_standings_df = race_results_df \
    .groupBy("race_year" ,  "team_name") \
    .agg(sum("points").alias("total_points") ,
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

#overwrite_partition(final_df, "f1_presentation", "constructor_standings", "race_year")

merge_condition = "tgt.team_name = src.team_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')


# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table f1_presentation.constructor_standings;
# MAGIC
# MAGIC select count(*) from f1_presentation.constructor_standings
# MAGIC

# COMMAND ----------

#********The above code is equivalent to the following code*****
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

display(final_df.filter("race_year = 2020").orderBy("total_points", ascending=False))

# COMMAND ----------

dbutils.notebook.exit("success")
