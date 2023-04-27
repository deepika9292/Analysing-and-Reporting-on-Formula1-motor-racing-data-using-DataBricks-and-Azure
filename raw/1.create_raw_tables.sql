-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;

CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt DOUBLE,
url STRING
) USING csv
OPTIONS (path "abfss://raw@formula1dl1dl.dfs.core.windows.net/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ##### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;

CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId STRING,
name STRING,
date DATE,
time STRING,
url STRING
) USING csv
OPTIONS (path "abfss://raw@formula1dl1dl.dfs.core.windows.net/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##### create constructors table
-- MAGIC 
-- MAGIC * Single Line JSON
-- MAGIC 
-- MAGIC * Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
) USING json
OPTIONS (path "abfss://raw@formula1dl1dl.dfs.core.windows.net/constructors.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##### create drivers table
-- MAGIC 
-- MAGIC * Single Line JSON
-- MAGIC 
-- MAGIC * Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
) USING json
OPTIONS (path "abfss://raw@formula1dl1dl.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##### create pitstops table
-- MAGIC 
-- MAGIC * MultiLine JSON
-- MAGIC 
-- MAGIC * Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
) USING json
OPTIONS (path "abfss://raw@formula1dl1dl.dfs.core.windows.net/pit_stops.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##### create table for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ###### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT

) USING csv
OPTIONS (path "abfss://raw@formula1dl1dl.dfs.core.windows.net/lap_times")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ###### Create Lap Times Table
-- MAGIC * json file
-- MAGIC * multiLine json files
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING

) USING json
OPTIONS (path "abfss://raw@formula1dl1dl.dfs.core.windows.net/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

