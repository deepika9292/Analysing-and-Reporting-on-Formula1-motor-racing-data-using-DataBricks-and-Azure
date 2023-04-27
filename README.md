# Analysing Formula1 motor racing data using DataBricks and PySpark

**About Formula1 Race**

During a Formula 1 race, drivers compete against each other on a track that consists of multiple laps. The winner is the driver who completes all the laps first and crosses the finish line.

Qualifying sessions are held prior to the race to determine the starting positions of the drivers. The driver with the fastest time in qualifying starts the race from pole position.

At the start of the race, drivers accelerate from a standing start and try to gain an advantage over their competitors. They must follow specific rules regarding overtaking and avoiding collisions.

Throughout the race, drivers make pit stops to change tires, refuel, and make adjustments to their cars. The number of pit stops and the timing of these stops can have a significant impact on the race outcome.

The race is typically divided into a predetermined number of laps, with drivers being awarded points based on their finishing position. The driver with the most points at the end of the season is crowned the Formula 1 World Champion.

**Approach on anlayizing Formula1 Data**

This project focuses on building a solution architecture for a data engineering solution using Azure Databricks, Azure Data Lake Gen2, Azure Data Factory, and Power BI. It covers topics such as creating and using Azure Databricks service, working with Databricks notebooks, configuring and monitoring Databricks clusters, using Delta Lake to implement a solution using Lakehouse architecture, and creating pipelines in Azure Data Factory to execute Databricks notebooks.

Perofrmed data ingestion and transformation using PySpark and Spark SQL in Azure Databricks, implemented a solution for Lakehouse architecture using Delta Lake, and create Azure Data Factory pipelines to execute Databricks notebooks. Connected Azure Databricks to Power BI to create reports.

**Data Source**
http://ergast.com/mrd/

**Data Ingestion Requirement**

![alt text](https://github.com/deepika9292/Analysing-and-Reporting-on-Formula1-motor-racing-data-using-DataBricks-and-Azure/blob/main/Screen%20Shot%202023-04-27%20at%2010.59.36%20AM.png)

* Ingest All 8 files into the data lake
* Ingested data must have the schema applied
* Ingested data must have audit columns
* Ingested data must be stored in columnar format (i.e., Parquet)
* Must be able to analyze the ingested data via SQL
* Ingestion logic must be able to handle incremental load

**Data Transformation Requirements**

* Join the key information required for reporting to create a new table. 
* Join the key information required for Analysis to create a new table.
* Transformed tables must have audit columns
* Must be able to analyze the transformed data via SQL
* Transformed data must be stored in columnar format (i.e., Parquet)
* Transformation logic must be able to handle incremental load

**Reporting Requirements**

 * Driver Standings
 * Constructor Standings

**Analysis Requirements**

 * Create Databricks Dashboards for Dominant Drivers
 * Create Databricks Dashboards for Dominant Teams
 
 **Scheduling Requirements**
 
 * Scheduled to run every Sunday 10PM
 * Ability to monitor pipelines
 * Ability to re-run failed pipelines
 * Ability to set-up alerts on failures
 
 **Other Non-Functional Requirements**
 
 * Ability to delete individual records
 * Ability to see history and time travel
 * Ability to roll back to a previous version

**Solution Architecture

![alt text](https://github.com/deepika9292/Analysing-and-Reporting-on-Formula1-motor-racing-data-using-DataBricks-and-Azure/blob/main/Screen%20Shot%202023-04-27%20at%2010.25.51%20AM.png)


