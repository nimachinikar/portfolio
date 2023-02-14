# Databricks notebook source
# MAGIC %md ## Ingest csv file 

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

# MAGIC %md 
# MAGIC ##### Step 1- Read CSV file using the Spark df reader

# COMMAND ----------

from pyspark.sql.functions import col, row_number, trim, when, lit, countDistinct, current_timestamp, concat, to_timestamp
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, DoubleType, DateType


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Importing Race.csv

# COMMAND ----------

#creating df Schema for race
df_schema = StructType ([StructField('raceId',IntegerType(),False),\
                        StructField('year',IntegerType(),True ),\
                        StructField('round',IntegerType(),True),\
                        StructField('circuitId',IntegerType(),True),\
                        StructField('name',StringType(),True),\
                        StructField('date',DateType(),True),\
                        StructField ('time',StringType(),True),\
                        StructField('url',StringType(),True)\
                       ])

# COMMAND ----------

#Importing races csv
df_race = spark.read.csv(f'{raw_folder_path}/{v_file_date}/races.csv',header=True, schema=df_schema)

# COMMAND ----------

#Verifying Schema
df_race.printSchema()

# COMMAND ----------

#Visualize data
display(df_race)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 : adding ingestion date and race_timestamp

# COMMAND ----------

# Adding new column
df_race= df_race.withColumn('race_timestamp',\
                                to_timestamp(\
                                              concat(df_race.date.cast(StringType()),lit(' '),df_race.time)\
                                             ,'yyyy-MM-dd HH:mm:ss'\
                                            )\
                           )\
                .withColumn('data_source',lit(v_data_source))\
                .withColumn('file_date',lit(v_file_date))


# COMMAND ----------

#Adding new column with loading time
df_race=add_ingestion_date(df_race)

# COMMAND ----------

df_race=df_race.select(col('raceId').alias('race_id')\
                       ,col('year').alias('race_year')\
                       ,col('round')\
                       ,col('circuitId').alias('circuit_id')\
                       ,col('name'),col('date')\
                       ,col('time')\
                       ,col('race_timestamp')\
                       ,col('data_source')\
                       ,col('ingestion_date')\
                       ,col('file_date'))

# COMMAND ----------

#Display output
display(df_race)

# COMMAND ----------

#output
df_race.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit('Success')
