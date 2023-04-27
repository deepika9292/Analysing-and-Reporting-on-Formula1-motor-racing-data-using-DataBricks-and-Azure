# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Drivers file.json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step 1: Read the JSON file using the spark DataFrame reader API

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])

# COMMAND ----------

driver_schema=StructType(fields=[StructField("driverId", IntegerType(), False),
                                 StructField("driverRef", StringType(), True),
                                StructField("number", IntegerType(), True),
                                StructField("code", StringType(), True),
                                StructField("name", name_schema),
                                StructField("dob", DateType(), True),
                                StructField("nationality", StringType(), True),
                                StructField("url", StringType(), True)])

# COMMAND ----------

drivers_df=spark.read \
.schema(driver_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step 2: Rename columns and add new column
# MAGIC 
# MAGIC <body> <ol> <li>driverid renamed to driver_id 
# MAGIC 
# MAGIC <li>driverRef renamed to driver_ref
# MAGIC <li>ingestion date added
# MAGIC <li>name added with concatenation of forename and surname
# MAGIC </ol></body>

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp,lit

# COMMAND ----------

drivers_with_columns_df=drivers_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("driverRef","driver_ref") \
.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

drivers_with_columns_df=add_ingestion_date(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step 3: Drop the unwanted columns
# MAGIC 
# MAGIC <body> <ol> <li>name.forename
# MAGIC 
# MAGIC <li>name.surname
# MAGIC <li>url
# MAGIC </ol></body>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Write output to processed container in parquet format

# COMMAND ----------

drivers_final_df=drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")