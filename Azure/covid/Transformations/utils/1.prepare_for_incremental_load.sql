-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS covid_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS covid_processed
LOCATION "/mnt/adlcovid23/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS covid_lookup CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/adlcovid23/lookup";
