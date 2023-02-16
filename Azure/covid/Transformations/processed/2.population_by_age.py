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

from pyspark.sql.functions import col, lit, concat, to_timestamp,max,min, regexp_replace, split
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, DoubleType, TimestampType


# COMMAND ----------

## Circuit

# COMMAND ----------

df_population = spark.read.csv(f'{raw_folder_path}/population_by_age/population_by_age.csv', sep='|', header=True)

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

# MAGIC %md
# MAGIC #### Step 2: Data Transformation

# COMMAND ----------

#spliting column in two: country and age group and removing PC_ 

# COMMAND ----------

df_population=df_population.withColumn('age_group',regexp_replace(split(df_population['indic_de,geo\\time'],',')[0],'PC_',''))\
                            .withColumn('country_code_2_digit',split(df_population['indic_de,geo\\time'],',')[1])\
                            .select('age_group','country_code_2_digit',col('2019 ').alias('pop_2019'))

# COMMAND ----------

df_population.createOrReplaceTempView('df_population')


# COMMAND ----------

#Using SQL to pivot table

# COMMAND ----------

df_population_pivot= spark.sql("select country_code_2_digit , age_group \
                                       , cast(regexp_replace(pop_2019, '[a-z]', '') AS decimal(4,2)) as percentage_2019 \
                               From df_population \
           WHERE length(country_code_2_digit) = 2")\
        .groupBy('country_code_2_digit')\
        .pivot('age_group')\
        .sum("percentage_2019")\
        .orderBy('country_code_2_digit')


# COMMAND ----------

df_population_pivot.createOrReplaceTempView('df_population_pivot')

# COMMAND ----------

#Joining and renaming columns

# COMMAND ----------

df_population=df_population_pivot\
                  .join(df_countries,df_population.country_code_2_digit==df_countries.country_code_2_digit)\
                .select(df_countries.country, df_countries.country_code_2_digit, df_countries.country_code_3_digit, df_countries.population, df_population_pivot.Y15_24.alias('age_group_15_24'), df_population_pivot.Y25_49.alias('age_group_25_49'), df_population_pivot.Y50_64.alias('age_group_50_64'), df_population_pivot.Y65_79.alias('age_group_65_79'), df_population_pivot.Y80_MAX.alias('age_group_80_max'))

# COMMAND ----------

#Adding new column with transformation time
df_population=add_ingestion_date(df_population)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Exporting Result

# COMMAND ----------

df_population.write.mode('overwrite').format('delta').saveAsTable('covid_processed.population')
