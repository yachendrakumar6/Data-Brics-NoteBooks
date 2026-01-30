# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("JSON Formats").getOrCreate()

data = [
    (1, '{"name":"Alice","age":30,"dept":"IT","salary":50000}'),
    (2, '{"name":"Bob","age":null,"dept":"HR","salary":60000}'),
    (3, '{"name":"Charlie","age":35,"dept":"IT","salary":null}')
]

df = spark.createDataFrame(data, ["emp_id", "emp_json"])

schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("dept", StringType()),
    StructField("salary", IntegerType())
])

df_from_json = df.withColumn("emp_struct", from_json(col("emp_json"), schema))
display(df_from_json)

# COMMAND ----------

# proway to flattern multi nested json file

# df_json = df_from_json.select("emp_id",col("emp_struct.*"))
# display(df_json)

# df_tojson = df_from_json.select("emp_id",to_json(col("emp_struct")).alias("emp_json"))
# display(df_tojson)

# df_json = df_from_json.select(
#     "emp_id",
#     col("emp_struct.name"),
#     col("emp_struct.age"),
#     col("emp_struct.dept"),
#     col("emp_struct.salary")
# )
# display(df_json)

schema_of_json_df = df_from_json.select(
    schema_of_json(col("emp_json")).alias("json_schema")
)
display(schema_of_json_df)

# COMMAND ----------

schema_df = df_from_json.select(schema_of_json(col("emp_json")).alias("schema")).first()[0]

df_from_json.select(
    "emp_id",
    from_json(col("emp_json"), schema_df).alias("emp_struct")
).select("emp_id", "emp_struct.*").show()

