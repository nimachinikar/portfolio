# Databricks notebook source
html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
displayHTML(html)

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
              CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW race_result_updated
              AS
              SELECT races.race_year,
                     constructors.name AS team_name,
                     drivers.driver_id,
                     drivers.name AS driver_name,
                     races.race_id,
                     results.position,
                     results.points,
                     11 - results.position AS calculated_points
                FROM f1_processed.results 
                JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
                JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
                JOIN f1_processed.races ON (results.race_id = races.race_id)
               WHERE results.position <= 10
                 AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
              MERGE INTO f1_presentation.calculated_race_results tgt
              USING race_result_updated upd
              ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
              WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_dominant_drivers
# MAGIC AS
# MAGIC SELECT driver_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points,
# MAGIC        RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
# MAGIC   FROM f1_presentation.calculated_race_results
# MAGIC GROUP BY driver_name
# MAGIC HAVING COUNT(1) >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, 
# MAGIC        driver_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points
# MAGIC   FROM f1_presentation.calculated_race_results
# MAGIC  WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
# MAGIC GROUP BY race_year, driver_name
# MAGIC ORDER BY race_year, avg_points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, 
# MAGIC        driver_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points
# MAGIC   FROM f1_presentation.calculated_race_results
# MAGIC  WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
# MAGIC GROUP BY race_year, driver_name
# MAGIC ORDER BY race_year, avg_points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, 
# MAGIC        driver_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points
# MAGIC   FROM f1_presentation.calculated_race_results
# MAGIC  WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
# MAGIC GROUP BY race_year, driver_name
# MAGIC ORDER BY race_year, avg_points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from f1_presentation.calculated_race_results
