# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("WindowValueFunctions") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

data = [
    (1, "Alice", "IT", 25, "2020-01-10", 60000, "Bob", "ProjectA", "NY", "NJ"),
    (2, "Bob", "IT", 30, "2019-03-15", 80000, "Charles", "ProjectA", "NY", "NY"),
    (3, "Charles", "IT", 35, "2018-06-01", 100000, "Diana", "ProjectB", "NY", "CT"),
    (4, "Diana", "IT", 40, "2017-02-20", 120000, None, "ProjectB", "NY", "NJ"),
    (5, "Eve", "HR", 28, "2021-04-01", 50000, "Frank", "Hiring", "LA", "CA"),
    (6, "Frank", "HR", 38, "2016-08-12", 90000, None, "Hiring", "LA", "CA"),
    (7, "Grace", "HR", 32, "2019-11-23", 65000, "Frank", "Training", "LA", "NV"),
    (8, "Heidi", "HR", 26, "2022-01-15", 48000, "Frank", "Training", "LA", "CA"),
    (9, "Ivan", "Finance", 29, "2020-07-01", 70000, "Judy", "Audit", "TX", "TX"),
    (10, "Judy", "Finance", 45, "2015-05-10", 130000, None, "Audit", "TX", "TX"),
    (11, "Kevin", "Finance", 34, "2018-09-09", 90000, "Judy", "Tax", "TX", "OK"),
    (12, "Laura", "Finance", 27, "2021-10-10", 65000, "Judy", "Tax", "TX", "TX"),
    (13, "Mallory", "Sales", 31, "2019-02-01", 75000, "Oscar", "Retail", "SF", "CA"),
    (14, "Oscar", "Sales", 44, "2014-12-01", 140000, None, "Retail", "SF", "CA"),
    (15, "Peggy", "Sales", 29, "2020-06-18", 68000, "Oscar", "Online", "SF", "NV"),
    (16, "Quentin", "Sales", 36, "2017-03-03", 90000, "Oscar", "Online", "SF", "CA"),
    (17, "Rita", "IT", 27, "2021-09-09", 65000, "Bob", "ProjectC", "NY", "NJ"),
    (18, "Steve", "IT", 33, "2019-12-12", 85000, "Charles", "ProjectC", "NY", "NY"),
    (19, "Trent", "HR", 41, "2015-01-01", 95000, None, "Compliance", "LA", "CA"),
    (20, "Uma", "Finance", 26, "2022-05-05", 60000, "Judy", "Budget", "TX", "TX"),
]

columns = ["emp_id", "name", "dept", "age", "doj","salary", "manager", "project", "office_location", "home_address" ]

df = spark.createDataFrame(data, columns).withColumn("doj", F.to_date("doj"))

display(df)


# COMMAND ----------

salary_window = Window.partitionBy("dept").orderBy("salary")

full_window = Window.partitionBy("dept").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_lag = df.withColumn("prev_salary", F.lag("salary", 1).over(salary_window))
df_lead = df.withColumn("next_salary", F.lead("salary", 1).over(salary_window))


display(df_lag)
display(df_lead)


# COMMAND ----------

salary_window = Window.partitionBy("dept").orderBy("salary")

full_window = Window.partitionBy("dept").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_first = df.withColumn("lowest_salary_in_dept", F.first_value("salary").over(salary_window))
df_last = df.withColumn("highest_salary_in_dept", F.last_value("salary").over(salary_window))

display(df_first)
display(df_last)

# COMMAND ----------

full_window = Window.partitionBy("dept").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_nth = df.withColumn("second_highest_salary",F.nth_value("salary", 2).over(full_window))

display(df_nth)

# COMMAND ----------

salary_window = Window.partitionBy("dept").orderBy("salary")

full_window = Window.partitionBy("dept").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

final_df = df \
    .withColumn("prev_salary", F.lag("salary").over(salary_window)) \
    .withColumn("next_salary", F.lead("salary").over(salary_window)) \
    .withColumn("dept_min_salary", F.first_value("salary").over(salary_window)) \
    .withColumn("dept_max_salary", F.last_value("salary").over(full_window)) \
    .withColumn("dept_2nd_salary", F.nth_value("salary", 2).over(full_window))

display(final_df)
