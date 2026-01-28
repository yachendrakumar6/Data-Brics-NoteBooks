# Databricks notebook source
from pyspark.sql.functions import col

data = spark.read.format("csv").option("header", True).option("inferSchema", "true").load("/Volumes/workspace/default/adventure_works/HumanResource/HumanResources_Department.csv")
#df = data.toDF("DepartmentID", "Name", "GroupName", "ModifiedDate")
display(df)
#df_filtered = df.filter(col("_c2") == "Research and Development")
#display(df_filtered)

# COMMAND ----------

from pyspark.sql.functions import col, trim
#==========================
#using trim and col functions
#==========================

df_filtered1 = df.filter(trim(col("GroupName")) == "Research and Development")
display(df_filtered1)

# COMMAND ----------

#-----Column Selection & Projection----- 

from pyspark.sql.functions import col, current_timestamp, lit

data = spark.read.format("csv").option("header", True).option("inferSchema", "true").load("/Volumes/workspace/default/adventure_works/HumanResource/HumanResources_Department.csv")
df = data.toDF("DepartmentID", "Name", "GroupName", "ModifiedDate")

df.createOrReplaceTempView("department")
df_basic = df.select(
    "DepartmentID",
    "Name",
    "GroupName"
)
display(df_basic)

df_filtered = df_basic.filter(df_basic.GroupName == "Research and Development")
display(df_filtered)

df_renamed = (df_filtered
                .withColumnRenamed("DepartmentID", "dept_id")
                .withColumnRenamed("Name", "dept_name")
                .withColumnRenamed("GroupName", "dept_group"))
display(df_renamed)

df_audit = (
    df_renamed
    .withColumn("created_date", current_timestamp())
    .withColumn("source_system", lit("AdventureWorks"))
)
display(df_audit)

df_drop = df_audit.drop("source_system")
display(df_drop)

# COMMAND ----------

df = spark.read.format("csv").option("header", True).option("inferSchema", "true").load("/Volumes/workspace/default/adventure_works/HumanResource/HumanResources_Employee.csv")

#display(df)
from pyspark.sql.functions import col, upper

df_select_expr = df.selectExpr("JobTitle as job_title", "VacationHours * 1.5 as vacation_hours")
#display(df_select_expr)

df_upper_case = df_select_expr.selectExpr("upper(job_title)" ,"vacation_hours")
#display(df_upper_case)

df_case = df_upper_case.selectExpr("CASE WHEN vacation_hours >= 20.0 THEN 'Yes' ELSE 'No' END")
display(df_case)

