# Databricks notebook source
# ===============================
# CSV
# ===============================
df_csv = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/path/data.csv")

# ===============================
# JSON
# ===============================
df_json = spark.read.format("json").option("multiLine", "true").load("/path/data.json")

# ===============================
# PARQUET
# ===============================
df_parquet = spark.read.format("parquet").load("/path/data.parquet")

# ===============================
# DELTA (Databricks)
# ===============================
df_delta = spark.read.format("delta").load("/path/delta_table")

# ===============================
# AVRO
# ===============================
df_avro = spark.read.format("avro").load("/path/data.avro")

# ===============================
# ORC
# ===============================
df_orc = spark.read.format("orc").load("/path/data.orc")

# ===============================
# TEXT
# ===============================
df_text = spark.read.format("text").load("/path/data.txt")

# ===============================
# XML (External Package)
# ===============================
df_xml = spark.read.format("xml").option("rowTag", "record").load("/path/data.xml")

# ===============================
# JDBC (Database)
# ===============================
df_jdbc = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/dbname") \
    .option("dbtable", "employees") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# ===============================
# S3 / ADLS (Same formats)
# ===============================
df_s3 = spark.read.parquet("s3a://bucket-name/path/")

df_adls = spark.read.csv("abfss://container@account.dfs.core.windows.net/path/")

#-----------------------------------------------------------------------------------


# ===============================
# Reading files using direct format pattern
# ===============================

path = "/Volumes/workspace/default/adventure_works/HumanResource/"

spark.read.csv(path)
spark.read.json(path)
spark.read.parquet(path)
spark.read.text(path)
spark.read.orc(path)
