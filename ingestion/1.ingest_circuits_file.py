# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read the csv file using Spark Dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls abfss://raw@formula1dl1dl.dfs.core.windows.net/

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("Circuitid", IntegerType(), False),
                                 StructField("CircuitRef", StringType(), True),
                                 StructField("name", StringType(), True),
                                 StructField("location", StringType(), True),
                                 StructField("country", StringType(), True),
                                 StructField("lat", DoubleType(), True),
                                 StructField("lng", DoubleType(), True),
                                 StructField("alt", IntegerType(), True),
                                 StructField("url", StringType(), True)])

# COMMAND ----------

circuits_df=spark.read\
.option("header",True)\
.schema(circuits_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")


# COMMAND ----------

print(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Selecting columns from dataframe

# COMMAND ----------

#method 1 of selcting columns
circuits_selected_df = circuits_df.select("Circuitid","CircuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

#method 2 of selcting columns, we can alos create alias unlike previous method
circuits_selected_df = circuits_df.select(col("Circuitid"),col("CircuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Rename columns in the dataframe

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitid","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude") \
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### step 4: Ingest new column to dataframe

# COMMAND ----------

#from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

#circuits_final_df=circuits_renamed_df.withColumn("ingestion_date",current_timestamp()) \
#.withColumn("env",lit("Production"))

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### step 5 : Write data to datalakes as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")