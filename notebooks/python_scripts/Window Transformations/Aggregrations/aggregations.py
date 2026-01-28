# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

data = [
    (1, "Alice", "IT", "India", "2023-01-10", 50000, 5),
    (2, "Bob", "IT", "India", "2023-02-15", 60000, 7),
    (3, "Charlie", "HR", "India", "2023-03-20", 40000, 4),
    (4, "David", "HR", "USA", "2023-04-25", 45000, 6),
    (5, "Eva", "Finance", "USA", "2023-05-30", 70000, 8),
    (6, "Frank", "IT", "USA", "2023-06-12", 65000, 9),
    (7, "Grace", "Finance", "India", "2023-07-18", 72000, 8),
    (8, "Helen", "HR", "India", "2023-08-22", 42000, 5)
]

columns = ["emp_id", "name", "dept", "country", "join_date", "salary", "experience"]
df = spark.createDataFrame(data, columns)
df = df.withColumn("current_timestamp", current_timestamp())

display(df)

# COMMAND ----------

# df_sum = df.groupBy("dept").sum("salary")

df_avg = df.groupBy("dept").avg("salary")

df_count = df.groupBy("dept").count()

df_mix = df.groupBy("dept").agg(
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    count("*").alias("emp_count")
)
display(df_mix)

df_mix_multi = df.groupBy("dept", "country").agg(
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    round(avg("salary"), 2).alias("avg_salary")
).orderBy("dept", "country")
display(df_mix_multi)


# COMMAND ----------

df_country_sum = df.groupBy("dept").agg(
    sum(when(col("country") == "India", col("salary")).otherwise(0)).alias("india_salary"),
    sum(when(col("country") == "USA", col("salary")).otherwise(0)).alias("usa_salary")
)                        
display(df_country_sum)

# COMMAND ----------

df_roll_up = df.rollup(col("country"), col("dept")).agg(
    sum(col("salary")).alias("total_salary")
).orderBy(col("country").desc(), col("dept").desc())
display(df_roll_up)

df_cube = df.cube(col("country"), col("dept")).agg(
    sum(col("salary")).alias("total_salary")
).orderBy(col("country").desc(), col("dept").desc())
display(df_cube)

# COMMAND ----------

some_fnctn = df.groupBy("dept").agg(
    countDistinct("country").alias("unique_countries"),
    collect_list("salary").alias("salary_list"),
    collect_set("salary").alias("unique_salaries"),
    first("salary", True).alias("first_non_null_salary")
)
display(some_fnctn)
