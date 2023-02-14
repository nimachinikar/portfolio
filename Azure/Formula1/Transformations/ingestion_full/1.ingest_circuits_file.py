# Databricks notebook source
# MAGIC %md ## Ingest csv file 

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

raw_folder_path #to check if it's working

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 1- Read CSV file using the Spark df reader

# COMMAND ----------

from pyspark.sql.functions import col, row_number, trim, when, lit, countDistinct, concat, to_timestamp
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, DoubleType, DateType


# COMMAND ----------

#find location of our files and what is inside
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl2323/raw

# COMMAND ----------

## Circuit

# COMMAND ----------

#creating df Schema for circuits
df_schema = StructType ([StructField('circuitId',IntegerType(),False),\
                        StructField('circuitRef',StringType(),True ),\
                        StructField('name',StringType(),True),\
                        StructField('location',StringType(),True),\
                        StructField('country',StringType(),True),\
                        StructField('lat',DoubleType(),True),\
                        StructField ('lng',DoubleType(),True),\
                        StructField ('alt',IntegerType(),True),\
                        StructField('url',StringType(),True)\
                       ])

# COMMAND ----------

#read csv
df_circuits = spark.read.csv(f'{raw_folder_path}/circuits.csv',header=True,schema=df_schema)

# COMMAND ----------

#checking type of df_circuits 
type(df_circuits)

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

#verifying column type
df_circuits.printSchema()

# COMMAND ----------

#Renaming columns
df_circuits=df_circuits.withColumnRenamed('lat','Latitude')\
                .withColumnRenamed('lng','Longitude')\
                .withColumnRenamed('alt','Altitude')\
                .withColumnRenamed('circuitId','circuit_id')\
                .withColumnRenamed('circuitRef','circuit_ref')\
                .withColumn('data_source',lit(v_data_source))\
                .drop('url')         

# COMMAND ----------

#Adding new column with loading time
df_circuits=add_ingestion_date(df_circuits)

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

df_circuits.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

#visual validation next 3

# COMMAND ----------

#%fs
#ls mnt/formula1df2323/processed/circuits

# COMMAND ----------

#display(spark.read.parquet('/mnt/formula1df2323/processed/circuits'))

# COMMAND ----------

#%sql
#Select * from f1_processed.circuits
