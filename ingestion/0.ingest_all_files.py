# Databricks notebook source
var_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"prm_data_source": "https://ergast.com/mrd/db/#csv",  "prm_file_date": "2021-04-18"})

# COMMAND ----------

var_result

# COMMAND ----------

var_result = dbutils.notebook.run("2.ingest_races_file", 0, {"prm_data_source": "https://ergast.com/mrd/db/#csv",  "prm_file_date": "2021-04-18"})

# COMMAND ----------

var_result = dbutils.notebook.run("3.ingest_constructor_file", 0, {"prm_data_source": "https://ergast.com/mrd/db/#csv",  "prm_file_date": "2021-04-18"})

# COMMAND ----------

var_result = dbutils.notebook.run("4.ingest_driver_file", 0, {"prm_data_source": "https://ergast.com/mrd/db/#csv",  "prm_file_date": "2021-04-18"})

# COMMAND ----------

var_result = dbutils.notebook.run("5.ingest_results_file", 0, {"prm_data_source": "https://ergast.com/mrd/db/#csv",  "prm_file_date": "2021-04-18"})

# COMMAND ----------

var_result = dbutils.notebook.run("6.ingest_pitstops_file", 0, {"prm_data_source": "https://ergast.com/mrd/db/#csv",  "prm_file_date": "2021-04-18"})

# COMMAND ----------

var_result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"prm_data_source": "https://ergast.com/mrd/db/#csv",  "prm_file_date": "2021-04-18"})

# COMMAND ----------

var_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"prm_data_source": "https://ergast.com/mrd/db/#csv",  "prm_file_date": "2021-04-18"})

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table f1_processed.circuits;
# MAGIC refresh table f1_processed.races;
# MAGIC refresh table f1_processed.constructors;
# MAGIC refresh table f1_processed.drivers;
# MAGIC refresh table f1_processed.results;
# MAGIC refresh table f1_processed.pit_stops;
# MAGIC refresh table f1_processed.lap_times;
# MAGIC refresh table f1_processed.qualifying;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select  count(*) from f1_processed.qualifying 

# COMMAND ----------


