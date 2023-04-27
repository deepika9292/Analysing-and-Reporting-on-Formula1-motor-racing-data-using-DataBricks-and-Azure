# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ####Access Azure Data Lake using Cluster Scope Credentials
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

