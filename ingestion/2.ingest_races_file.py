# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read the csv file using Spark Dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls abfss://raw@formula1dl1dl.dfs.core.windows.net/2021-03-21/

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceid", IntegerType(), False),
                                 StructField("year", IntegerType(), True),
                                 StructField("round", IntegerType(), True),
                                 StructField("circuitid", StringType(), True),
                                 StructField("name", StringType(), True),
                                 StructField("date", DateType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("url", StringType(), True)])

# COMMAND ----------

races_df=spark.read\
.option("header",True)\
.schema(races_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Selecting columns from dataframe

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

#method 2 of selcting columns, we can alos create alias unlike previous method
races_selected_df = races_df.select(col("raceid"),col("year"),col("round"),col("circuitid"),col("name"),col("date"),col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Rename columns in the dataframe

# COMMAND ----------

races_renamed_df=races_selected_df.withColumnRenamed("raceid","race_id") \
.withColumnRenamed("year","race_year") \
.withColumnRenamed("circuitid","circuit_id") 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### step 4: Ingest new column to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit, to_timestamp, concat

# COMMAND ----------

#circuits_final_df=circuits_renamed_df.withColumn("ingestion_date",current_timestamp()) \
#.withColumn("env",lit("Production"))

races_final_df=races_renamed_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(' '),col("time")),'yyyy-MM-dd HH:mm:ss'))\
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

races_final_df=add_ingestion_date(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Now exclude columns data and time column from races dataframe

# COMMAND ----------

races_final_df = races_final_df.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("race_timestamp"),col("ingestion_date"),col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### step 5 : Write data to datalakes as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")