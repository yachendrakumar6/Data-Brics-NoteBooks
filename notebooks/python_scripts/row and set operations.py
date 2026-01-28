# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RowSetOpsLarge").getOrCreate()

data1 = [
    (1, "A", "IT", "BLR", 50000, 2020, "A"),
    (2, "B", "HR", "HYD", 40000, 2019, "A"),
    (3, "C", "IT", "BLR", 60000, 2021, "A"),
    (4, "D", "FIN", "CHN", 70000, 2018, "I"),
    (5, "E", "IT", "BLR", 50000, 2020, "A"),
    (6, "F", "HR", "HYD", 45000, 2019, "A"),
    (7, "G", "IT", "CHN", 65000, 2022, "A"),
    (8, "H", "FIN", "BLR", 72000, 2021, "A"),
    (9, "I", "IT", "HYD", 58000, 2020, "I"),
    (10, "J", "HR", "BLR", 42000, 2018, "A"),
    (3, "C", "IT", "BLR", 60000, 2021, "A"),   # duplicate
    (5, "E", "IT", "BLR", 50000, 2020, "A"),   # duplicate
    (11, "K", "FIN", "CHN", 80000, 2022, "A"),
    (12, "L", "IT", "HYD", 55000, 2021, "A"),
    (13, "M", "HR", "BLR", 47000, 2020, "A"),
    (14, "N", "FIN", "HYD", 75000, 2019, "I"),
    (15, "O", "IT", "BLR", 62000, 2022, "A"),
    (16, "P", "HR", "CHN", 48000, 2021, "A"),
    (17, "Q", "IT", "BLR", 67000, 2023, "A"),
    (18, "R", "FIN", "BLR", 82000, 2023, "A")
]

data2 = [
    (3, "C", "IT", "BLR", 60000, 2021, "A"),
    (5, "E", "IT", "BLR", 50000, 2020, "A"),
    (7, "G", "IT", "CHN", 65000, 2022, "A"),
    (8, "H", "FIN", "BLR", 72000, 2021, "A"),
    (10, "J", "HR", "BLR", 42000, 2018, "A"),
    (12, "L", "IT", "HYD", 55000, 2021, "A"),
    (14, "N", "FIN", "HYD", 75000, 2019, "I"),
    (19, "S", "HR", "BLR", 46000, 2022, "A"),
    (20, "T", "IT", "CHN", 69000, 2023, "A")
]

cols = ["emp_id", "name", "dept", "city", "salary", "joining_year", "status"]

df1 = spark.createDataFrame(data1, cols)
df2 = spark.createDataFrame(data2, cols)

#df1.filter("dept = 'IT'").distinct().show()

df_union = df1.union(df2)
# display(df_union)

df_distinct = df_union.distinct()
display(df_distinct)
