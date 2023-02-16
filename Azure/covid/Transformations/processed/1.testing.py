# Databricks notebook source
# MAGIC %md ## Ingest csv file 

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

from pyspark.sql.functions import col, row_number, trim, when, lit, countDistinct, concat, to_timestamp,max,min
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, DoubleType, TimestampType


# COMMAND ----------

## Circuit

# COMMAND ----------

#creating df Schema for testing
df_schema_testing = StructType ([StructField('country',StringType(),False),\
                        StructField('country_code',StringType(),True ),\
                        StructField('year_week',StringType(),True),\
                        StructField('new_cases',IntegerType(),True),\
                        StructField('tests_done',IntegerType(),True),\
                        StructField('population',IntegerType(),True),\
                        StructField ('testing_rate',DoubleType(),True),\
                        StructField ('positivity_rate',DoubleType(),True),\
                        StructField('testing_data_source',StringType(),True),\
                        StructField('LoadId',StringType(),True),\
                        StructField('AdlLoadTime',TimestampType(),True)\
                       ])

# COMMAND ----------

#read csv
df_testing = spark.read.csv(f'{raw_folder_path}/ecdc/testing.csv',header=True,schema=df_schema_testing)

# COMMAND ----------

#creating df Schema for countries
df_schema_countries = StructType ([StructField('country',StringType(),False),\
                        StructField('country_code_2_digit',StringType(),True ),\
                        StructField('country_code_3_digit',StringType(),True),\
                        StructField('continent',StringType(),True),\
                        StructField('population',IntegerType(),True)\
                                 ])

# COMMAND ----------

#read csv
df_countries = spark.read.csv(f'{lookup_folder_path}/dim_country/country_lookup.csv',header=True,schema=df_schema_countries)

# COMMAND ----------

#read csv
df_date = spark.read.csv(f'{lookup_folder_path}/dim_date/dim_date.csv',header=True)

# COMMAND ----------

#Transform date
df_date=df_date.withColumn('year_week',concat(col('year'),lit('-W'),col('week_of_year')))\
        .groupBy('year_week').agg(min('date').alias('weekStartDate'),\
                                max('date').alias('weekEndDate'))

# COMMAND ----------

df_testing=df_testing.join(df_date ,df_testing.year_week==df_date.year_week,'left')\
                  .join(df_countries,df_testing.country_code==df_countries.country_code_2_digit)\
                .select(df_testing.country,'country_code_2_digit','country_code_3_digit',df_testing.year_week,'weekStartDate','weekEndDate','new_cases','tests_done',df_testing.population,'testing_rate','positivity_rate', 'testing_data_source','LoadId', 'AdlLoadTime')

# COMMAND ----------

#Adding new column with transformation time
df_testing=add_ingestion_date(df_testing)

# COMMAND ----------

df_testing.write.mode('overwrite').format('delta').saveAsTable('covid_processed.testings')

# COMMAND ----------

#%sql
#Select * from covid_processed.testings
