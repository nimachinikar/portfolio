# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting Lap Times File

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 Ingest csv file result

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

from pyspark.sql.functions import col, row_number, trim, when, lit, countDistinct, concat, to_timestamp
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, DoubleType, DateType

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl2323/raw

# COMMAND ----------

df_schema=('raceId INT, driverId INT, lap INT, position INT,time STRING,milliseconds INT')

# COMMAND ----------

df_lapTimes=spark.read.schema(df_schema)\
                .csv(f'{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv')

# COMMAND ----------

display(df_lapTimes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 Transform df

# COMMAND ----------

# DBTITLE 1,Renaming + creation of ingestion date
df_lapTimes=df_lapTimes.withColumnRenamed('raceId','race_id')\
                     .withColumnRenamed('driverId','driver_id')\
                    .withColumn('data_source',lit(v_data_source))\
                    .withColumn('file_date',lit(v_file_date))


# COMMAND ----------

#Adding new column with loading time
df_lapTimes=add_ingestion_date(df_lapTimes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Export df

# COMMAND ----------

merge_condition = 'target.race_id = source.race_id AND target.driver_id = source.driver_id AND target.lap=source.lap AND target.race_id = source.race_id' 
merge_delta_data(df_lapTimes, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
