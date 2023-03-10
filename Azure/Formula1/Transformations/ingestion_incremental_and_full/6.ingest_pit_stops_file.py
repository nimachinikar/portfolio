# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting Pit Stop File

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 Ingest Json file result

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, row_number, trim, when, lit, countDistinct, current_timestamp, concat, to_timestamp
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, DoubleType, DateType

# COMMAND ----------

df_schema=('raceId INT, stop INT, lap INT, milliseconds INT, duration STRING, time STRING, driverId INT')

# COMMAND ----------

df_pitstops=spark.read.schema(df_schema)\
                .option('multiLine',True)\
                .json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

display(df_pitstops)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 Transform df

# COMMAND ----------

# DBTITLE 1,Renaming + creation of ingestion date
df_pitstops=df_pitstops.withColumnRenamed('raceId','race_id')\
                     .withColumnRenamed('driverId','driver_id')\
                    .withColumn('data_source',lit(v_data_source))\
                    .withColumn('file_date',lit(v_file_date))


# COMMAND ----------

#Adding new column with loading time
df_pitstops=add_ingestion_date(df_pitstops)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Export df

# COMMAND ----------

merge_condition = 'target.driver_id = source.driver_id AND target.stop=source.stop AND target.race_id = source.race_id' 

merge_delta_data(df_pitstops, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
