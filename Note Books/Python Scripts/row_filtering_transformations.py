# Databricks notebook source
data = [
    (1, "Alice", 29, "IT", 50000),
    (2, "Bob", 35, "HR", 60000),
    (3, "Charlie", 40, "IT", 80000),
    (4, "David", 23, "Finance", 45000),
    (5, "Eva", 31, "HR", 70000)
]

cols = ["id", "name", "age", "dept", "salary"]
df = spark.createDataFrame(data, cols)
display(df)

#only filter method
df_age = df.filter(df.age > 30)
display(df_age)

#only where method
df.where(df.dept == 'IT').show() 
        #or
df_where = df.where(df.dept == 'IT')
display(df_where)

#only limit method
df_limit = df.limit(3)
display(df_limit)

df.sample(False, 0.4).show()


# COMMAND ----------

data = [
    (1, "Alice", 29, "IT", 50000),
    (2, "Bob", 35, "HR", 60000),
    (3, "Charlie", 40, "IT", 80000),
    (4, "David", 23, "Finance", 45000),
    (5, "Eva", 31, "HR", 70000)
]

cols = ["id", "name", "age", "dept", "salary"]
df = spark.createDataFrame(data, cols)

from pyspark.sql.functions import col

df_filtered = df.filter(col("age") > 30)
display(df_filtered)

df_multi_filter = df.filter((col("age") > 30) & (col("dept") == "HR"))
display(df_multi_filter)

df_multi_filter1 = df.filter((col("dept") == "IT") | (col("dept") == "Finance"))
display(df_multi_filter1)

#isin function
df_isin = df.filter(col("dept").isin("IT", "HR"))
display(df_isin)

#between function
df_between = df.filter(col("salary").between(60000, 90000))
display(df_between)

# COMMAND ----------

#cheat sheet for row filter functions

data = [
    (1, "Alice", 29, "IT", 50000),
    (2, "Bob", 35, "HR", 60000),
    (3, "Charlie", 40, "IT", 80000),
    (4, "David", 23, "Finance", 45000),
    (5, "Eva", 31, "HR", 70000)
]

cols = ["id", "name", "age", "dept", "salary"]
df = spark.createDataFrame(data, cols)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col

df_highest_salary = df.withColumn("rn", row_number().over(Window.partitionBy("dept").orderBy(col("salary").desc()))).filter(col("rn") == 1)
display(df_highest_salary)
