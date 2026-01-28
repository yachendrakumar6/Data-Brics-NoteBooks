# Databricks notebook source
df = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/default/adventure_works/HumanResource/HumanResources_Department.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").save("/Volumes/workspace/default/adventure_works/delta/HumanResources_Department/")
