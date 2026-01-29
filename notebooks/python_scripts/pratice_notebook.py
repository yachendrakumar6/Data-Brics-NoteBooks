# Databricks notebook source
from pyspark.sql.functions import col
data = [
    (1, "Alice", 29, "IT", 50000),
    (2, "Bob", 35, "HR", 60000),
    (3, "Charlie", 40, "IT", 80000),
    (4, "David", 23, "Finance", 45000),
    (5, "Eva", 31, "HR", 70000),
    (6, "Frank", 45, "HR", 55000),
    (7, "Grace", 38, "Finance", 75000),
    (8, "Helen", 26, "IT", 65000),
    (9, "Ian", 31, "HR", 52000),
    (10, "Jack", 50, "IT", 100000)
]

cols = ["id", "name", "age", "dept", "salary"]
df = spark.createDataFrame(data, cols)

# display(df)

# df_multi =  df.filter(df.dept == "IT").where(df.age > 25).orderBy(df.age.desc())
# display(df_multi)

df_multi1 = df.select(
    df.id, 
    col("age") > 25
)
display(df_multi1)
