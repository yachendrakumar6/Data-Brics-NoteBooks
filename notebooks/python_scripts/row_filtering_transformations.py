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
    (5, "Eva", 31, "HR", 70000),
    (6, "Frank", "HR", 45, 55000),
    (7, "Grace", "Finance", 38, 75000),
    (8, "Helen", "IT", 26, 65000),
    (9, "Ian", "HR", 31, 52000),
    (10, "Jack", "IT", 50, 100000)
]

cols = ["id", "name", "age", "dept", "salary"]
df = spark.createDataFrame(data, cols)

df_mutli = df.filter((df.age > 30) & (df.dept == 'IT'))
display(df_mutli)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col

df_highest_salary = df.withColumn("rn", row_number().over(Window.partitionBy("dept").orderBy(col("salary").desc()))).filter(col("rn") == 1)
display(df_highest_salary)

from pyspark.sql.functions import col, lit, current_date

final_df = (
    df
    .select("emp_id", "name", "salary")
    .withColumn("salary", col("salary") + 2000)
    .withColumns({
        "annual_salary": col("salary") * 12,
        "bonus": col("salary") * 0.10,
        "country": lit("INDIA"),
        "processed_date": current_date()
    })
    .withColumnRenamed("emp_id", "employee_id")
    .drop("name")                                          
    .toDF("employee_id", "salary", "annual_salary", "bonus", "country", "processed_date")
)

display(final_df)

df_filtered = (
    df
    .filter(df.age > 30)
    .where(df.dept == "IT")
    .limit(3)
)

df_filtered.show()

