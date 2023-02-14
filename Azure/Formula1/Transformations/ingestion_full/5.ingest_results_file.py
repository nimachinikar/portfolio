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

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, row_number, trim, when, lit, countDistinct, current_timestamp, concat, to_timestamp
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, DoubleType, DateType

# COMMAND ----------

df_schema=('constructorId INT, driverId INT, fastestLap INT, fastestLapSpeed FLOAT, fastestLapTime STRING, grid INT, laps INT, milliseconds INT, points INT, position INT, positionOrder int, positionText STRING, raceId INT, rank INT, resultId INT, statusId INT, time STRING')

# COMMAND ----------

df_results=spark.read.schema(df_schema).json('/mnt/formula1dl2323/raw/results.json')

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
                     .drop('statusId')

# COMMAND ----------

#Adding new column with loading time
df_results=add_ingestion_date(df_results)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Export df

# COMMAND ----------

df_results.write.mode('overwrite').partitionBy('race_id').format('delta').saveAsTable('f1_processed.results')

# COMMAND ----------

dbutils.notebook.exit('Success')
