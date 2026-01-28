# Databricks notebook source
df = spark.read.format("csv").load("/Volumes/workspace/default/adventure_works/HumanResource/")

spark.sql("USE CATALOG delta_tables")




# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --CREATE CATALOG IF NOT EXISTS delta_tables;
# MAGIC --USE CATALOG delta_tables;
# MAGIC CREATE SCHEMA IF NOT EXISTS HumanResource;
# MAGIC CREATE SCHEMA IF NOT EXISTS Sales;
# MAGIC CREATE SCHEMA IF NOT EXISTS Person;
# MAGIC CREATE SCHEMA IF NOT EXISTS Production;
# MAGIC CREATE SCHEMA IF NOT EXISTS Purchase;
# MAGIC
# MAGIC

# COMMAND ----------

source_path = "/Volumes/workspace/default/adventure_works/HumanResource/"

spark.sql("USE CATALOG delta_tables");
spark.sql("USE SCHEMA humanresource");


files = dbutils.fs.ls(source_path)

for f in files:
    if f.name.endswith(".csv"):
        # Remove prefix and extension
        table_name = (
            f.name
            .replace("HumanResources_", "")
            .replace(".csv", "")
        )

        file_path = f.path

        print(f"Creating table: {table_name}")

        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)

        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)

spark.sql("SHOW TABLES")


# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG delta_tables;
# MAGIC USE SCHEMA humanresource;
# MAGIC
# MAGIC SELECT * FROM employee

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG delta_tables;
# MAGIC USE SCHEMA humanresource;
