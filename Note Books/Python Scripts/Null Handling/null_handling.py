# Databricks notebook source
# MAGIC %md
# MAGIC **Detecting Nulls**: Use isNull() to find null values.
# MAGIC **Dropping Nulls**: Use dropna() to remove rows with nulls (all or specific columns).
# MAGIC **Filling Nulls**: Use fillna() to replace nulls with defaults.
# MAGIC **Coalesce Function**: Use coalesce() to return the first non-null value.
# MAGIC **Aggregations**: Use coalesce() in aggregations to handle nulls safely.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

spark = SparkSession.builder.appName("NullHandling").getOrCreate()

data = [
    (1, "Alice", "IT", 90000, None),
    (2, "Bob", None, None, "Bangalore"),
    (3, None, "HR", 60000, None),
    (4, "David", "IT", None, "Hyderabad"),
    (5, None, None, None, None)
]

columns = ["emp_id", "name", "dept", "salary", "location"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

display(df.na.fill("UNKNOWN"))

# COMMAND ----------

display(
    df.na.fill({
    "name": "UNKNOWN",
    "dept": "NOT KNOWN",
    "salary": 0,
    "location": "NULL"
})
)


# COMMAND ----------

display(df.na.drop())

display(df.na.drop(how="all"))

display(df.na.drop(subset=["salary", "dept"]))

# how="any" → default
# how="all" → only fully NULL rows dropped

# COMMAND ----------

display(df.na.replace(
    to_replace={"IT": "Information Technology", "HR": "Human Resources"}
))


# COMMAND ----------

from pyspark.sql.functions import col, coalesce

df.select(
    "emp_id",
    coalesce(
        col("location"),
        col("dept"),
        col("name"),
        col("emp_id").cast("string")
    ).alias("fallback_value")
).show()


# COMMAND ----------

df.filter(col("salary").isNull()).show()

# COMMAND ----------

display(df.filter(col("salary").isNotNull()))
