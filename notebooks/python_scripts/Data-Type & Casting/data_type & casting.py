# Databricks notebook source
data = [
    (1, "100", "50000.75", "2024-01-01"),
    (2, "200", "60000.50", "2024-02-01")
]

cols = ["id", "age", "salary", "doj"]

df = spark.createDataFrame(data, cols)

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df_casted = df.withColumn("age", col("age").cast("int"))\
                .withColumn("salary", col("salary").cast("double"))\
                .withColumn("doj", col("doj").cast("date"))
display(df_casted)

# COMMAND ----------

df_astype = df \
    .withColumn("age", col("age").astype("int")) \
    .withColumn("salary", col("salary").astype("double"))
display(df_astype)
