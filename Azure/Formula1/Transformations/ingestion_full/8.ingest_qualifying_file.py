# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting Qualifying File

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 Ingest json files result

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

# MAGIC %fs
# MAGIC ls /mnt/formula1dl2323/raw

# COMMAND ----------

df_schema=('qualifyId INT, raceId INT, driverId INT, constructorId INT,number INT, position INT, q1 STRING,q2 STRING, q3 STRING')

# COMMAND ----------

df_qualifying=spark.read.schema(df_schema)\
                .option('multiLine',True)\
                .json(f'{raw_folder_path}/qualifying')

# COMMAND ----------

display(df_qualifying)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 Transform df

# COMMAND ----------

# DBTITLE 1,Renaming + creation of ingestion date
df_qualifying=df_qualifying.withColumnRenamed('raceId','race_id')\
                     .withColumnRenamed('driverId','driver_id')\
                     .withColumnRenamed('qualifyId','qualify_id')\
                     .withColumnRenamed('constructorId','constructor_id')\
                     .withColumn('data_source',lit(v_data_source))


# COMMAND ----------

#Adding new column with loading time
df_qualifying=add_ingestion_date(df_qualifying)

# COMMAND ----------

display(df_qualifying)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Export df

# COMMAND ----------

df_qualifying.write.mode('overwrite').format('delta').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

dbutils.notebook.exit('Success')
