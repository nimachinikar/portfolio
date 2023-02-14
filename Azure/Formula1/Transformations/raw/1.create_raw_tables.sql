-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Import CSV

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ####Create circuit table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT,
circuitRef STRING,
name STRING ,
location STRING, 
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv 
OPTIONS (path '/mnt/formula1dl2323/raw/circuits.csv', header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ####Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId Int,
year Int,
round Int ,
circuitId Int, 
name String,
date Date,
time String,
url  String
)
USING CSV
OPTIONS (path '/mnt/formula1dl2323/raw/races.csv', header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Import JSON

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ####Create constructor table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT
, constructorRef STRING
, name STRING
, nationality STRING
, url STRING
)
USING JSON
OPTIONS (path '/mnt/formula1dl2323/raw/constructors.json', header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ####Create drivers table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
code STRING
, dob DATE
, driverId INT
, driverRef STRING
, name STRUCT<forename:STRING,surname:STRING>
, nationality STRING
, number STRING
, url STRING
)
USING JSON
OPTIONS (path '/mnt/formula1dl2323/raw/drivers.json', header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ####Create results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
constructorId INT, driverId INT, fastestLap INT, fastestLapSpeed FLOAT, fastestLapTime STRING, grid INT, laps INT, milliseconds INT, points INT, position INT, positionOrder int, positionText STRING, raceId INT, rank INT, resultId INT, statusId INT, time STRING
)
USING JSON
OPTIONS (path '/mnt/formula1dl2323/raw/results.json', header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Import Multi Line Json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ####Create pit stop table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
raceId INT, stop INT, lap INT, milliseconds INT, duration STRING, time STRING, driverId INT
)
USING JSON
OPTIONS (path '/mnt/formula1dl2323/raw/pit_stops.json', multiLine true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Import File CSV and Json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ####Create lap times table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT, driverId INT, lap INT, position INT,time STRING,milliseconds INT)
USING csv
OPTIONS (path '/mnt/formula1dl2323/raw/lap_times/lap_times_split*.csv',header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ####Create qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT, raceId INT, driverId INT, constructorId INT,number INT, position INT, q1 STRING,q2 STRING, q3 STRING)
USING json
OPTIONS (path '/mnt/formula1dl2323/raw/qualifying', multiLine true);

-- COMMAND ----------

select * from f1_raw.qualifying
