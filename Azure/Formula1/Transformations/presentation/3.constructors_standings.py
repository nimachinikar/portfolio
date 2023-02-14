# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc,  rank, asc
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ######Find race years for which the data needs to be processed

# COMMAND ----------

df_race_results = spark.read.format('delta').load(f"{presentation_folder_path}/race_results")\
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

list_race_year=df_column_to_list(df_race_results,'race_year')

# COMMAND ----------

df_race_results = spark.read.format('delta').load(f"{presentation_folder_path}/race_results")\
                             .filter(col('race_year').isin(list_race_year))


# COMMAND ----------

# Result per year and team
df_teams_position=df_race_results.groupBy('race_year','team').agg(sum('points').alias('total_points'),count(when(col('position')==1, True)).alias('number_wins'))

# COMMAND ----------

#ranking per year

# COMMAND ----------

ranking_teams=Window.partitionBy('race_year').orderBy(desc('total_points'),('number_wins'))

# COMMAND ----------

df_team_rank = df_teams_position.withColumn("rank", rank().over(ranking_teams))

# COMMAND ----------

merge_condition = 'target.team = source.team AND target.race_year = source.race_year'
merge_delta_data(df_team_rank, 'f1_presentation', 'team_standings', presentation_folder_path, merge_condition, 'race_year')
