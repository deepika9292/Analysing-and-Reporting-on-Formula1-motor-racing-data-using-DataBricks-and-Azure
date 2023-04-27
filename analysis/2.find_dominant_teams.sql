-- Databricks notebook source
SELECT team_name,
count(1) as total_races,
sum(calculated_points) AS total_points,
avg(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team_name
having count(1) >=50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT team_name,
count(1) as total_races,
sum(calculated_points) AS total_points,
avg(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
where race_year between 2001 and 2010
GROUP BY team_name
having count(1) >=100
ORDER BY avg_points DESC

-- COMMAND ----------

