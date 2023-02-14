# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import col, row_number, trim, when, lit, countDistinct, concat, to_timestamp, current_timestamp
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, DoubleType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ####Loading Data

# COMMAND ----------

df_circuits = spark.read.format('delta').load(f'{processed_folder_path}/circuits')\
                    .withColumnRenamed('location', 'circuit_location') 


# COMMAND ----------

df_results = spark.read.format('delta').load(f'{processed_folder_path}/results')\
                        .withColumnRenamed('time', 'race_time')\
                        .filter(f"file_date = '{v_file_date}'")


# COMMAND ----------

df_drivers= spark.read.format('delta').load(f'{processed_folder_path}/drivers')\
                       .withColumnRenamed('number', 'driver_number') \
                       .withColumnRenamed('name', 'driver_name') \
                       .withColumnRenamed('nationality', 'driver_nationality') 

# COMMAND ----------

df_constructors = spark.read.format('delta').load(f'{processed_folder_path}/constructors') \
                            .withColumnRenamed('name', 'team') 

# COMMAND ----------

df_races= spark.read.format('delta').load(f'{processed_folder_path}/races')\
                        .withColumnRenamed('name', 'race_name') \
                        .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming data

# COMMAND ----------

# MAGIC %md 
# MAGIC #####joining tables

# COMMAND ----------

df_sub_circuits=df_races.join(df_circuits, df_races.circuit_id==df_circuits.circuit_id)\
                            .select(df_races.race_id,df_circuits.circuit_location)


# COMMAND ----------

#Adding constructors and driver info and race
df_races_result=df_results.join(df_drivers, df_results.driver_id == df_drivers.driver_id) \
                         .join(df_constructors, df_results.constructor_id == df_constructors.constructor_id)\
                         .join(df_races, df_results.race_id ==df_races.race_id )\
                         .join(df_circuits.select(df_circuits.circuit_id,df_circuits.circuit_location), df_races.circuit_id == df_circuits.circuit_id)\
                         .select(df_races.race_id,'race_year', 'race_name', 'race_date', 'circuit_location' , df_drivers.driver_id, 'driver_name','driver_number', 'driver_nationality','team', 'grid', 'fastest_lap', 'race_time', 'points', 'position',df_results.file_date)\
                          .withColumn('created_date', current_timestamp())

# COMMAND ----------

display(df_races_result)

# COMMAND ----------

#Display result
display(df_races_result.filter((df_races_result.race_year == 2020) & (df_races_result.race_name.contains('Abu'))).orderBy(df_races_result.points.desc()))


# COMMAND ----------

merge_condition = 'target.race_id = source.race_id AND target.driver_id = source.driver_id AND target.race_id = source.race_id'
merge_delta_data(df_races_result, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results
# MAGIC order by race_id desc
