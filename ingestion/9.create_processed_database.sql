-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@formula1dl1dl.dfs.core.windows.net/"

-- COMMAND ----------

desc database f1_processed

-- COMMAND ----------

