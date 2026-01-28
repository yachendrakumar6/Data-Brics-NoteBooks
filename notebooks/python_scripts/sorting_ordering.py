# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (101, "A", "IT", 70000, 2021),
    (102, "B", "HR", 50000, 2020),
    (103, "C", "IT", 90000, 2023),
    (104, "D", "Finance", 60000, 2021),
    (105, "E", "HR", None, 2022),
    (106, "F", "IT", 80000, None)
]

columns = ["emp_id", "name", "dept", "salary", "year"]

df = spark.createDataFrame(data, columns)

# COMMAND ----------

from pyspark.sql.functions import col

#orderBy function with ascending order on name column
df_order = df.orderBy(col("name").asc())
display(df_order)

#orderBy function with descending order on salary column
df_order1 = df.orderBy(col("salary").desc())
display(df_order1)

#multicolumn filtering nulls to last using function desc_nulls_last
df.orderBy(
    col("dept").asc(),
    col("salary").desc_nulls_first(),
    col("year").asc_nulls_last()
).show()

df_order_both = df.orderBy(col("dept").asc(), col("salary").asc())
display(df_order_both)

# COMMAND ----------

#sort
df_sort1 = df.sort("salary")
display(df_sort1)

df_sort2 = df.sort(col("name").asc(), col("salary").desc())
display(df_sort2)

df.sortWithinPartitions("dept").show()

