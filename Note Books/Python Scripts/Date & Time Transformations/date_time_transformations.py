# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC to_date(col, format)
# MAGIC to_timestamp(col, format)
# MAGIC
# MAGIC date_add(col,10) 10 means after days
# MAGIC date_sub(col,5) before 5 days
# MAGIC
# MAGIC datediff(end_date, start_date)
# MAGIC
# MAGIC months_between(end_date, start_date)
# MAGIC
# MAGIC
# MAGIC COMMON MISTAKES (Interview Killers ❌)
# MAGIC ❌ Using date_add on string
# MAGIC ❌ Comparing date as string
# MAGIC ❌ Wrong format in to_date
# MAGIC ❌ Expecting months_between to return int
# MAGIC ❌ Forgetting null handling
# MAGIC
# MAGIC Interview Q&A
# MAGIC ⭐ to_date vs date_format
# MAGIC → to_date = string ➜ date
# MAGIC → date_format = date ➜ string
# MAGIC
# MAGIC ⭐ datediff return type?
# MAGIC → Integer (days)
# MAGIC
# MAGIC ⭐ Can date_add work on timestamp?
# MAGIC → ❌ No (convert to date first)
# MAGIC
# MAGIC ⭐ Best practice?
# MAGIC → Convert early → calculate → format at end

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("datetime").getOrCreate()

data = [
    (1, "Alice", "2023-01-15", "2023-01-15 10:30:00"),
    (2, "Bob", "2022-12-01", "2022-12-01 18:45:10"),
    (3, "Charlie", "2021-06-20", "2021-06-20 09:15:55"),
    (4, "David", None, None)
]

df = spark.createDataFrame(data, ["id", "name", "joining_date_str", "login_ts_str"])

display(df)

# COMMAND ----------

df_to_date = df.withColumn("joining_date",to_date("joining_date_str", "yyyy-MM-dd"))
display(df_to_date)

df_to_timestamp = df.withColumn("login_ts",to_timestamp("login_ts_str", "yyyy-MM-dd HH:mm:ss"))
display(df_to_timestamp)


df_mix1 = df.select("name","joining_date_str",
                    date_add("joining_date_str", 10).alias("joining_date_plus10"),
                    date_sub("joining_date_str", 5).alias("joining_date_minus5")
                    )
display(df_mix1)

df_diff = df.withColumn("experience_days",date_diff(current_date(), "joining_date_str"))
display(df_diff)

df_months_between = df.withColumn("months_difference", round(months_between(current_date(), "joining_date_str")))
display(df_months_between)

df_add_months = df.select("name","joining_date_str",
                          add_months("joining_date_str", 3).alias("adding_3months"),
                          add_months("joining_date_str", -2).alias("subtracting_2months")
                          )
display(df_add_months)

df_mix2 = df.select("name","joining_date_str",
    year("joining_date_str").alias("Year"),
    month("joining_date_str").alias("Month"),
    dayofmonth("joining_date_str").alias("Day")
)
display(df_mix2)

df_date_format = df.select(
    "name",
    date_format("joining_date_str", "dd-MMM-yyyy").alias("formatted_date"),
    date_format("login_ts_str", "yyyy/MM/dd HH:mm").alias("formatted_ts")
)
display(df_date_format)

df_current_datetime = df.withColumn("today", current_date()) \
         .withColumn("now", current_timestamp())
display(df_current_datetime)


# COMMAND ----------

# df_date_null = df.withColumn(
#     "status",
#     when(col("joining_date_str").isNull(), "UNKNOWN")
#     .otherwise("ACTIVE")
# )
# display(df_date_null)

df_prod_level = df.withColumn(
    "status",
    when(datediff(current_date(), col("joining_date_str")) > 365, "Experienced")
    .otherwise("Fresher")
)

display(df_prod_level)
