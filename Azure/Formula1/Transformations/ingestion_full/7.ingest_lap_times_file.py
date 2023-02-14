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
                .csv(f'{raw_folder_path}/lap_times/lap_times_split*.csv')

# COMMAND ----------

display(df_lapTimes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 Transform df

# COMMAND ----------

# DBTITLE 1,Renaming + creation of ingestion date
df_lapTimes=df_lapTimes.withColumnRenamed('raceId','race_id')\
                     .withColumnRenamed('driverId','driver_id')\
                    .withColumn('data_source',lit(v_data_source))


# COMMAND ----------

#Adding new column with loading time
df_lapTimes=add_ingestion_date(df_lapTimes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Export df

# COMMAND ----------

df_lapTimes.write.mode('overwrite').format('delta').saveAsTable('f1_processed.lap_times')

# COMMAND ----------

dbutils.notebook.exit('Success')
