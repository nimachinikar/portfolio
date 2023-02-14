-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION '/mnt/formula1dl2323/processed';

-- COMMAND ----------

desc database f1_raw;

-- COMMAND ----------

desc database f1_processed;
