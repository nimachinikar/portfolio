# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df=input_df.withColumn('ingestion_date',current_timestamp())
    return output_df

# COMMAND ----------

def re_arranged_partitionkey(df_input,partition_key): 
    column_list=[]
    for col in df_input.schema.names:
        if col != partition_key:
            column_list.append(col)
    column_list.append(partition_key)
    df_output = df_input.select(column_list)
    return df_output

# COMMAND ----------

def overwrite_partition (df_input, db_name, table_name, partition_key):
    df_output=re_arranged_partitionkey(df_input, partition_key)
    
    spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
    if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
        df_output.write.mode('overwrite').insertInto(f'{db_name}.{table_name}')
    else:
        df_output.write.mode('overwrite').partitionBy(partition_key).format('parquet').saveAsTable(f'{db_name}.{table_name}')


# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("target").merge(
        input_df.alias("source"),
        merge_condition) \
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
  else:
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list (input_df,col):
    list_col = input_df.select(col)\
            .distinct()\
            .collect()
    list_race_year=[]
    for element in list_col:
        list_race_year.append(element[col])
    return list_race_year
    
