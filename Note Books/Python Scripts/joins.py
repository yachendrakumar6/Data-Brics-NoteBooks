# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("JoinsExample").getOrCreate()

emp_data = [
    Row(emp_id=1, emp_name="Alice", emp_salary=50000, emp_dept_id=101, emp_location="New York"),
    Row(emp_id=2, emp_name="Bob", emp_salary=60000, emp_dept_id=102, emp_location="Los Angeles"),
    Row(emp_id=3, emp_name="Charlie", emp_salary=55000, emp_dept_id=101, emp_location="Chicago"),
    Row(emp_id=4, emp_name="David", emp_salary=70000, emp_dept_id=103, emp_location="San Francisco"),
    Row(emp_id=5, emp_name="Eve", emp_salary=48000, emp_dept_id=102, emp_location="Houston"),
    Row(emp_id=6, emp_name="Hudson", emp_salary=95000, emp_dept_id=106, emp_location="Washington DC")
]

dept_data = [
    Row(dept_id=101, dept_name="Engineering", dept_head="John", dept_location="New York"),
    Row(dept_id=102, dept_name="Marketing", dept_head="Mary", dept_location="Los Angeles"),
    Row(dept_id=103, dept_name="Finance", dept_head="Frank", dept_location="Chicago"),
    Row(dept_id=104, dept_name="Advertising", dept_head="Murray", dept_location="Calfornia")
]

emp_columns = ["emp_id", "emp_name", "emp_salary", "emp_dept_id", "emp_location"]
dept_columns = ["dept_id", "dept_name", "dept_head", "dept_location"]

emp_df = spark.createDataFrame(emp_data, emp_columns)
dept_df = spark.createDataFrame(dept_data, dept_columns)

display(emp_df)
display(dept_df)

# COMMAND ----------

project_data = [
    Row(project_id=1001, dept_id=101, project_name="AI Platform"),
    Row(project_id=1002, dept_id=102, project_name="Ad Campaign"),
    Row(project_id=1003, dept_id=104, project_name="Internal Tool")  # NO matching dept
]

project_columns = ["project_id", "dept_id", "project_name"]

project_df = spark.createDataFrame(project_data, project_columns)
# display(project_df)

#inner_join
inner_join = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id , "inner")
display(inner_join)

#left_join
left_join = inner_join.join(project_df, inner_join.dept_id == project_df.dept_id, "left")
display(left_join)

#right_join
right_join = dept_df.join(project_df, dept_df.dept_id == project_df.dept_id, "right")
display(right_join)

#left_anti
left_anti_df = emp_df.join(dept_df, emp_df.emp_dept_id == dept_df.dept_id, "left_anti")
display(left_anti_df)

#There will be no right_semi or right_anti joins directly here we just need interchange or swap df names
#right_anti
right_anti_df = dept_df.join(emp_df, emp_df.emp_dept_id == dept_df.dept_id, "left_anti")
display(right_anti_df)

df_filtered_select = emp_df.join(dept_df, emp_df["emp_dept_id"] == dept_df["dept_id"], "inner") \
    .select(emp_df.emp_id, emp_df.emp_name, emp_df.emp_salary, dept_df.dept_name, dept_df.dept_location) \
    .filter("emp_salary >= 55000")
display(df_filtered_select)

# COMMAND ----------

#outer joins

full_outer_df = dept_df.join(project_df, dept_df.dept_id == project_df.dept_id, "full_outer")
display(full_outer_df)

#left side everything right side will be shown null if record not found
left_outer_df = dept_df.join(project_df, dept_df.dept_id == project_df.dept_id, "left_outer")
display(left_outer_df)

#right side everything left side will not be shown anything if record not found
right_outer_df = dept_df.join(project_df, dept_df.dept_id == project_df.dept_id, "right_outer")
display(right_outer_df)
