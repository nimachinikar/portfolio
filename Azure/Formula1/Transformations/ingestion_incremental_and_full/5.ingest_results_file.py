# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting Result File

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

df_schema=('constructorId INT, driverId INT, fastestLap INT, fastestLapSpeed FLOAT, fastestLapTime STRING, grid INT, laps INT, milliseconds INT, points INT, position INT, positionOrder int, positionText STRING, raceId INT, rank INT, resultId INT, statusId INT, time STRING')

# COMMAND ----------

df_results=spark.read.schema(df_schema).json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

display(df_results)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 Transform df

# COMMAND ----------

# DBTITLE 1,Removing column + renaming + creation of ingestion date
df_results=df_results.withColumnRenamed('resultId','result_id')\
                     .withColumnRenamed('raceId','race_id')\
                     .withColumnRenamed('driverId','driver_id')\
                     .withColumnRenamed('constructorId','constructor_id')\
                     .withColumnRenamed('positionText','position_text')\
                     .withColumnRenamed('positionOrder','position_order')\
                     .withColumnRenamed('fastestLap','fastest_lap')\
                     .withColumnRenamed('fastestLapTime','fastest_lap_time')\
                     .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
                     .withColumn('data_source',lit(v_data_source))\
                     .withColumn('file_date',lit(v_file_date))\
                     .drop('statusId')

# COMMAND ----------

#Adding new column with loading time
df_results=add_ingestion_date(df_results)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Clean df

# COMMAND ----------

df_results = df_results.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 Export df

# COMMAND ----------

merge_condition = 'target.result_id = source.result_id AND target.race_id = source.race_id' #Add joining key & partition key 
merge_delta_data(df_results, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
