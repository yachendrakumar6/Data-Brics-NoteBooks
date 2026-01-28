# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("emp_name", StringType()),
    StructField("dept", StringType()),
    StructField("age", IntegerType()),
    StructField("doj", StringType()),
    StructField("salary", IntegerType()),
    StructField("manager", StringType()),
    StructField("project", StringType()),
    StructField("office_location", StringType()),
    StructField("home_city", StringType()),
    StructField("employment_type", StringType()),
    StructField("status", StringType())
])

data = [
 (1,"Alice","IT",29,"2021-06-01",90000,"John","AI","Bangalore","Chennai","FT","Active"),
 (2,"Bob","IT",31,"2020-03-15",90000,"John","ML","Bangalore","Hyderabad","FT","Active"),
 (3,"Charlie","IT",27,"2022-01-10",85000,"John","AI","Bangalore","Pune","FT","Active"),
 (4,"David","HR",35,"2019-05-21",70000,"Sara","Hiring","Mumbai","Mumbai","FT","Active"),
 (5,"Eva","HR",32,"2020-07-11",70000,"Sara","Payroll","Mumbai","Pune","FT","Active"),
 (6,"Frank","HR",29,"2023-02-01",65000,"Sara","Hiring","Mumbai","Delhi","CT","Active"),
 (7,"Grace","Finance",41,"2018-11-01",120000,"Mark","Audit","Delhi","Delhi","FT","Active"),
 (8,"Hank","Finance",38,"2019-08-17",110000,"Mark","Tax","Delhi","Noida","FT","Active"),
 (9,"Ivy","Finance",36,"2020-09-09",110000,"Mark","Audit","Delhi","Gurgaon","FT","Active"),
 (10,"Jack","Sales",28,"2022-06-15",75000,"Tom","CRM","Chennai","Trichy","FT","Active"),
 (11,"Kate","Sales",30,"2021-04-20",80000,"Tom","CRM","Chennai","Madurai","FT","Active"),
 (12,"Leo","Sales",33,"2019-12-01",82000,"Tom","CRM","Chennai","Salem","FT","Active"),
 (13,"Mona","IT",34,"2018-03-12",105000,"John","Cloud","Bangalore","Mysore","FT","Active"),
 (14,"Nina","IT",26,"2023-01-01",78000,"John","AI","Bangalore","Kolar","CT","Active"),
 (15,"Oscar","Finance",45,"2016-06-06",130000,"Mark","Strategy","Delhi","Delhi","FT","Active"),
 (16,"Paul","HR",39,"2017-08-08",90000,"Sara","Policy","Mumbai","Thane","FT","Active"),
 (17,"Quinn","Sales",29,"2023-05-05",70000,"Tom","CRM","Chennai","Erode","CT","Active"),
 (18,"Rose","IT",37,"2017-10-10",115000,"John","Security","Bangalore","Coorg","FT","Active"),
 (19,"Steve","Finance",34,"2021-01-19",98000,"Mark","Risk","Delhi","Faridabad","FT","Active"),
 (20,"Tina","Sales",31,"2020-02-02",88000,"Tom","CRM","Chennai","Vellore","FT","Active")
]

df = spark.createDataFrame(data, schema).withColumn("doj", to_date(col("doj"), "yyyy-MM-dd"))
display(df)


# COMMAND ----------

#partition by dept and order by salary desc
dept_salary_partition = Window.partitionBy("dept","manager").orderBy(col("salary").desc())

#row_number
df_row_number = df.withColumn("row_number", row_number().over(dept_salary_partition))
display(dept_salary)

# another way for performing the operation
df_row_number1 = df.select("*",row_number().over(dept_salary_partition).alias("row_number"))

df_rank = df.withColumn("rank", rank().over(dept_salary_partition))

df_percent_rank = df.withColumn("percent_rank", percent_rank().over(dept_salary_partition))

df_dense_rank = df.withColumn("dense_rank", dense_rank().over(dept_salary_partition))

df_ntile = df.withColumn("salary_band", ntile(4).over(dept_salary_partition))



display(df_row_number)
display(df_row_number1)
display(df_rank)
display(df_percent_rank)
display(df_dense_rank)
display(df_ntile)
