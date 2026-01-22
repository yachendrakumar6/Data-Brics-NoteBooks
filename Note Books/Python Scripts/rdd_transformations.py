# Databricks notebook source
# DBTITLE 1,tranformations
from pyspark.sql.types import *
builtin_sum = sum  # Save Python's built-in sum before it gets overwritten
from pyspark.sql.functions import *

df_data = spark.read.format('csv').option("header", "true").option("inferSchema", "True").load("/Volumes/db_vinay/dev/input/data.csv")
rdd1= df_data.rdd

rdd2=rdd1.map(lambda x:(x[1]))


rdd3= rdd2.flatMap(lambda line:line.split(" "))


# for words in rdd3.collect():
#     print(words)
rdd4=rdd3.map(lambda x: (x,1))
# for x in rdd4.collect():
#     print(x)
rdd5 = rdd4.groupByKey()


rdd6 = rdd5.map(lambda x:(x[0],builtin_sum(x[1])))
for x in rdd6.collect():
    print(x)

# rdd6= rdd4.reduceByKey(lambda x,y:x+y)
# for x in rdd6.collect():
#     print(x)


# COMMAND ----------

# DBTITLE 1,Cell 2


# rdd = sc.textFile("/Volumes/vinay_catalog/vinay_schema/landing/HumanResources_Department.csv")
# rdd=spark.read.option('header', 'true').csv('/Volumes/vinay_catalog/vinay_schema/landing/HumanResources_Department.csv').rdd

data=[1,2,3,4,5,6]
rdd=spark.sparkContext.parallelize(data, numSlices=1)
# rdd=spark.read.csv("/Volumes/vinay_catalog/vinay_schema/landing/HumanResources_Department.csv", header=True).rdd
rdd.getNumPartitions()
# rdd.count()
# rdd.collect()

# rdd1=rdd.map(lambda x:(x[1]))
# rdd1= rdd.map(lambda x:(x[0],int(x[0]) * 2))
# rdd1= rdd.map(lambda x:(x[0],x[1].upper()))
# rdd1=rdd.map(lambda x:(x[0], len(x[1])))
# rdd1.collect()

# COMMAND ----------

##  wordcount  ##
data = [
    "hello spark",
    "spark is fast",
    "hello big data"
]
rdd = sc.parallelize(data)

rdd1=rdd.flatMap(lambda x: x.split())
rdd2=rdd1.map(lambda word:(word,1))
# rdd2=rdd.flatMap(lambda x: x.split())
rdd3=rdd2.reduceByKey(lambda x,y:x+y)

rdd3.sortByKey().collect()
# rdd2.collect()

# COMMAND ----------

rdd=spark.read.csv("/Volumes/vinay_catalog/vinay_schema/landing/HumanResources_Department.csv", header=True).rdd
rdd1=rdd.filter(lambda x: x[1] == "Engineering")
rdd1.collect()

# COMMAND ----------

df_data = spark.read.format('csv').option("header", "true").option("inferSchema", "True").load("/Volumes/db_vinay/dev/input/data.csv")
# display(df_data)
rdd_data=df_data.rdd
# rdd_data.collect()
# for x in rdd_data.collect():
#     print(x)
rdd_map= rdd_data.map(lambda x:x[1])
# for x in rdd_map.collect():
#     print(x)
rdd_flatmap=rdd_map.flatMap(lambda x:x.split(" "))
# for x in rdd_flatmap.collect():
#     print(x)
rdd_map1=rdd_flatmap.map(lambda x:(x,1))
# for x in rdd_map1.collect():
#     print(x)
# rdd_group=rdd_map1.groupByKey().mapValues(lambda x:sum(x))

# for x in rdd_group.collect():
#     print(x)

rdd_reducebykey=rdd_map1.reduceByKey(lambda x,y:x+y)
rdd_map2= rdd_reducebykey.map(lambda x:(x[1],x[0]))
# for x in rdd_map2.collect():
#     print(x)
rdd_sorted = rdd_map2.sortByKey(ascending=False)
rdd_top2= rdd_sorted.take(2)
print(rdd_top2)
# for x in rdd_top2:
#     print(x)







# COMMAND ----------


