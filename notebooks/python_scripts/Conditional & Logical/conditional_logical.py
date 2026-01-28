# Databricks notebook source
# MAGIC %md
# MAGIC **SYNTAX**
# MAGIC
# MAGIC when(condition, value).otherwise(value)
# MAGIC when(cond1, val1).when(cond2, val2).otherwise(val3)
# MAGIC Always add .otherwise() → avoids NULL surprises
# MAGIC expr(CASE WHEN) 
# MAGIC expr("ifnull(column, replacement)")
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ConditionalLogical").getOrCreate()

data = [
    (1, "Alice", "IT", 75000, 5000, None),
    (2, "Bob", "HR", 45000, None, "A"),
    (3, "Charlie", "IT", None, 2000, "I"),
    (4, "David", "Finance", 90000, None, None),
    (5, "Eva", "HR", 30000, 1000, "A"),
    (6, "Frank", "IT", None, None, None)
]

schema = ["emp_id", "emp_name", "dept", "salary", "bonus", "status"]

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

df_when = df.withColumn("salary_band", 
                        when(col("salary") >= 80000, "High")
                        .when(col("salary") >= 50000, "Medium")
                        .otherwise("Low")
                        )
display(df_when)

df_when1 = df.withColumn(
    "bonus_eligible",
    when((col("dept") == "IT") & (col("salary") >= 60000), "eligible")
    .when((col("dept") == "HR") & (col("salary") >= 40000), "eligible")
    .otherwise("ineligible")
)
display(df_when1)

df_when2 = df.withColumn(
    "salary_status",
    when(col("salary").isNull(), "not disclosed")
    .otherwise("disclosed")
)
display(df_when2)

df_expr = df.withColumn(
    "grade",
    expr("""
        CASE 
            WHEN salary >= 80000 THEN 'A'
            WHEN salary >= 60000 THEN 'B'
            ELSE 'C'
        END
    """)
)
display(df_expr)

df_expr_ifnull = df.withColumn(
    "salary_filled",
    expr("ifnull(salary, 0)")
)
display(df_expr_ifnull)

df.withColumn(
    "salary_filled",
    coalesce(col("salary"), lit(0))
).display()

df_coalsece = df.withColumn(
    "final_status",
    when(col("status").isNull(), "UNKNOWN")
    .when(col("status") == "A", "ACTIVE")
    .otherwise("INACTIVE")
)
display(df_coalsece)

df_when_coalesce = df.withColumn(
    "final_salary",
    when(coalesce(col("bonus"), lit(0)) > 5000, "High")
    .otherwise("Normal")
)
display(df_when_coalesce)
