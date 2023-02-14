# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingesting Driver Json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 reading json file

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, row_number, trim, when, lit, countDistinct, current_timestamp, concat, to_timestamp
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, DoubleType, DateType

# COMMAND ----------

spark.read.json(f'{raw_folder_path}/{v_file_date}/drivers.json').printSchema()

# COMMAND ----------

#option 1
name_schema=StructType ([StructField('forname',StringType(),True),\
                        StructField('surname',StringType(),True )])
                         
df_schema = StructType ([StructField('code',StringType(),False),\
                        StructField('dob',DateType(),True ),\
                        StructField('driverId',IntegerType(),True),\
                        StructField('driverRef',StringType(),True),\
                        StructField('name',name_schema),\
                        StructField('nationality',StringType(),True),\
                        StructField ('number',IntegerType(),True),\
                        StructField('url',StringType(),True)\
                       ])

# COMMAND ----------

display(spark.read.schema(df_schema).json(f'{raw_folder_path}/{v_file_date}/drivers.json'))

# COMMAND ----------

#option 2
df_schema='code STRING, dob DATE, driverId INT, driverRef STRING, name STRUCT<forename:STRING,surname:STRING>, nationality STRING, number STRING, url STRING'

# COMMAND ----------

df_drivers=spark.read.schema(df_schema).json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

df_drivers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 Transforming data

# COMMAND ----------

df_drivers =df_drivers.withColumnRenamed('driverId','driver_id')\
                      .withColumnRenamed('driverRef','driver_ref')\
                      .drop('url')\
                      .withColumn('name',concat('name.forename',lit(' '),'name.surname'))\
                      .withColumn('data_source',lit(v_data_source))\
                      .withColumn('file_date',lit(v_file_date))


# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# Step 3 Exporting Data

# COMMAND ----------

df_drivers.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit('Success')
