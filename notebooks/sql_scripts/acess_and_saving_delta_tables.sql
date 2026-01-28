-- Databricks notebook source
-- MAGIC %python
-- MAGIC df = spark.read.format("csv").load("/Volumes/workspace/default/adventure_works/HumanResource/")
-- MAGIC
-- MAGIC spark.sql("USE CATALOG delta_tables")
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------


--CREATE CATALOG IF NOT EXISTS delta_tables;
--USE CATALOG delta_tables;
CREATE SCHEMA IF NOT EXISTS HumanResource;
CREATE SCHEMA IF NOT EXISTS Sales;
CREATE SCHEMA IF NOT EXISTS Person;
CREATE SCHEMA IF NOT EXISTS Production;
CREATE SCHEMA IF NOT EXISTS Purchase;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC source_path = "/Volumes/workspace/default/adventure_works/HumanResource/"
-- MAGIC
-- MAGIC spark.sql("USE CATALOG delta_tables");
-- MAGIC spark.sql("USE SCHEMA humanresource");
-- MAGIC
-- MAGIC
-- MAGIC files = dbutils.fs.ls(source_path)
-- MAGIC
-- MAGIC for f in files:
-- MAGIC     if f.name.endswith(".csv"):
-- MAGIC         # Remove prefix and extension
-- MAGIC         table_name = (
-- MAGIC             f.name
-- MAGIC             .replace("HumanResources_", "")
-- MAGIC             .replace(".csv", "")
-- MAGIC         )
-- MAGIC
-- MAGIC         file_path = f.path
-- MAGIC
-- MAGIC         print(f"Creating table: {table_name}")
-- MAGIC
-- MAGIC         df = spark.read.format("csv") \
-- MAGIC             .option("header", "true") \
-- MAGIC             .option("inferSchema", "true") \
-- MAGIC             .load(file_path)
-- MAGIC
-- MAGIC         df.write.format("delta") \
-- MAGIC             .mode("overwrite") \
-- MAGIC             .saveAsTable(table_name)
-- MAGIC
-- MAGIC spark.sql("SHOW TABLES")
-- MAGIC

-- COMMAND ----------

USE CATALOG delta_tables;
USE SCHEMA humanresource;

SELECT * FROM employee

-- COMMAND ----------

USE CATALOG delta_tables;
USE SCHEMA humanresource;
