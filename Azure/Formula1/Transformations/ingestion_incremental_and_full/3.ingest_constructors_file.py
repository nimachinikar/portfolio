# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors json file

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

#DDL style
df_schema='constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

df_constructor = spark.read.schema(df_schema).json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

display(df_constructor)

# COMMAND ----------

df_constructor.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 Data Transformation

# COMMAND ----------

df_constructor= df_constructor.drop('url')\
                              .withColumnRenamed('constructorId','constructor_id')\
                              .withColumnRenamed('constructorRef','constructor_ref')\
                               .withColumn('data_source',lit(v_data_source))\
                               .withColumn('file_date',lit(v_file_date))


# COMMAND ----------

#Adding new column with loading time
df_constructor=add_ingestion_date(df_constructor)

# COMMAND ----------

display(df_constructor)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 Exporting file to json

# COMMAND ----------

df_constructor.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit('Success')
