# Databricks notebook source
from pyspark.sql import functions as F

data = [
    (1, ["java", "spark", "sql","java"]),
    (2, []),
    (3, None),
    (4, ["python", None, "spark"]),
]

df = spark.createDataFrame(data, ["id", "skills"])
display(df)


# COMMAND ----------

#size
df_select = df.select(F.size("skills").alias("skills"))
display(df_select)

#array_contains
df_array_contains = df.select("id", F.array_contains("skills", "spark").alias("has_spark"))
display(df_array_contains)

#slice
df_slice = df.select("id", F.slice("skills", 1, 2).alias("sliced_skills"))
display(df_slice)

#sort_array
df_sort = df.select("id", F.sort_array("skills", asc = False) )
display(df_sort)

#explode
df_explode = df.select("id", F.explode("skills").alias("skill"))
display(df_explode)

#explode_outer
df_explode_outer = df.select("id", F.explode_outer("skills").alias("skill"))
display(df_explode_outer)

#posexplode_outer
df_pos_explode = df.select("id", F.posexplode_outer("skills"))
display(df_pos_explode)

#array_distinct
df_distinct = df.select(F.array_distinct("skills"))
display(df_distinct)

#element_at
df_mix = df.select(
    "id",
    F.when(
        F.size("skills") > 0,
        F.element_at("skills", F.size("skills"))
    ).otherwise(None).alias("last_skill")
)
display(df_mix)

