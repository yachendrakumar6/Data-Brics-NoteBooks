# Databricks notebook source
data = [
    (1, "Alice", 29, "IT", 50000),
    (2, "Bob", 35, "HR", 60000),
    (3, "Charlie", 40, "IT", 80000),
    (4, "David", 23, "Finance", 45000),
    (5, "Eva", 31, "HR", 70000),
    (6, "Frank", "HR", 45, 55000),
    (7, "Grace", "Finance", 38, 75000),
    (8, "Helen", "IT", 26, 65000),
    (9, "Ian", "HR", 31, 52000),
    (10, "Jack", "IT", 50, 100000)
]

cols = ["id", "name", "age", "dept", "salary"]
df = spark.createDataFrame(data, cols)

df_multi =  df.filter(df.age > 30).where(df.dept == "IT").orderBy(df.age.desc()).limit(2)
df_multi.show()
