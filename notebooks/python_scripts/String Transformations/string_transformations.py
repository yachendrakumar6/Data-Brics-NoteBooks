# Databricks notebook source
# MAGIC %md
# MAGIC **Index for String Functions**
# MAGIC **Syntax**
# MAGIC
# MAGIC **Name Will be Converted into UPPER CASE**
# MAGIC upper()   -> upper(col("emp_name")).alias("uppercase_name")
# MAGIC
# MAGIC **Name Will be Converted into LOWER CASE**
# MAGIC lower()   -> lower(col("emp_name")).alias("lowercase_name")
# MAGIC
# MAGIC **Name Will be Capitalized First Letter Even After Space**
# MAGIC initcap() -> initcap(col("emp_name")).alias("titlecase_name")
# MAGIC
# MAGIC **Length of the string(name or email or any other string)**
# MAGIC length()  -> length(col("email")).alias("email_length")
# MAGIC
# MAGIC **splitting into two parts with any specialized characters (@,_-)**
# MAGIC split()   -> split(col("address"), "-").alias("split_address")
# MAGIC
# MAGIC **Extract partial string from a column**
# MAGIC substring() → substring(col("address"), 1, 9).alias("city_part")
# MAGIC
# MAGIC **Combining Multiple Strings at a time**
# MAGIC concat() -> concat(col("emp_name"), lit(" - "), col("department")).alias("info")
# MAGIC
# MAGIC **Combining Multiple Strings at a time**
# MAGIC concat_ws() → concat_ws(" | ", col("emp_name"), col("department"), col("email")).alias("profile")
# MAGIC #====================================================================
# MAGIC **concat() **
# MAGIC _concat() is NULL-sensitive_
# MAGIC Concatenates multiple columns as-is
# MAGIC Does NOT ignore NULL values
# MAGIC
# MAGIC **concat_ws()** 
# MAGIC _concat_ws() is NULL-safe and separator-based_
# MAGIC Concatenates columns using a separator
# MAGIC Skips NULL values automatically
# MAGIC
# MAGIC **always use concat_ws() unless you explicitly need NULL propagation**
# MAGIC #====================================================================
# MAGIC **Find this pattern and replace it**
# MAGIC regexp_replace() -> regexp_replace(column, pattern, replacement)
# MAGIC
# MAGIC **Find this pattern and give me ONLY that**
# MAGIC regexp_extract() -> regexp_extract(column, pattern, group_index)
# MAGIC #====================================================================
# MAGIC trim()  → "alice   johnson"     -> _removing left and right sides white spaces_
# MAGIC ltrim() → "alice   johnson   "  -> _removing left sides white spaces_
# MAGIC rtrim() → "   alice   johnson"  -> _removing right sides white spaces_

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StringFunctions").getOrCreate()

data = [
    (1,  "  alice johnson ", "it department",        "alice@gmail.com",        "Bangalore-560001"),
    (2,  "BOB smith",        "hr department",        "bob@yahoo.com",          "Chennai-600001"),
    (3,  "charlie brown",    "finance department",   "charlie@outlook.com",    "Hyderabad-500001"),
    (4,  "  david lee",      "it department",        "david@gmail.com",        "Pune-411001"),
    (5,  None,               "sales department",     None,                     "Mumbai-400001"),
    (6,  "emma watson",      "marketing department", "emma@company.com",       "Delhi-110001"),
    (7,  " FRANK MILLER ",   "IT department",        "frank@hotmail.com",      "Noida-201301"),
    (8,  "george",           "support department",   "george@gmail.com",       "Kolkata-700001"),
    (9,  "  henry ford",     "finance department",   "henry@ford.com",         "Ahmedabad-380001"),
    (10, "Isla Fisher",      "HR department",        "isla@yahoo.com",         "Jaipur-302001"),
    (11, " jack ma ",        "sales department",     "jack@alibaba.com",       "Gurgaon-122001"),
    (12, "KATE WINSLET",     "marketing department", "kate@gmail.com",         "Indore-452001"),
    (13, " leo messi",       "sports department",    "leo@psg.com",            "Mumbai-400002"),
    (14, "   michael scott", "management department","michael@dundermifflin.com","Scranton-18503"),
    (15, "nancy drew",       "IT department",        None,                     "Bangalore-560078")
]

cols = ["emp_id", "emp_name", "department", "email", "address"]

df = spark.createDataFrame(data, cols)
display(df)


# COMMAND ----------

from pyspark.sql.functions import *

df_upper = df.select(col("emp_name"), upper(col("emp_name")).alias("uppercase_name"))
display(df_upper)

df_lower = df.select(col("emp_name"), lower(col("emp_name")).alias("lowercase_name"))
display(df_lower)

df_initcap = df.select(col("emp_name"), initcap(col("emp_name")).alias("proper_name"))
display(df_initcap)

df_length = df.select(col("email"), length(col("email")).alias("email_length"))
display(df_length)

df_split = df.select(col("address"), split(col("address"), "-").alias("split_address"))
display(df_split)

df_splitator = df.select(
    split(col("address"), "-")[0].alias("CITY"),
    split(col("address"), "-")[1].alias("PINCODE")
)
display(df_splitator)

df_substring = df.select(col("address"), substring(col("address"), 1, 8).alias("city_part"))
display(df_substring)

df_concat = df.select(concat(col("emp_name"), lit(" - "), col("department")).alias("full_info"))
display(df_concat)

df_concatws = df.select(concat_ws(" | ", col("emp_name"), col("department"), col("email")).alias("profile"))
display(df_concatws)

df_trim = df.select(trim(col("emp_name")).alias("clean_name"))
display(df_trim)

df_ltrim = df.select(ltrim("emp_name").alias("left_clean_name"))
display(df_ltrim)

df_rtrim = df.select(rtrim("emp_name").alias("right_clean_name"))
display(df_ltrim)

df_regexp_replace = df.select(col("address"),regexp_replace(col("address"), "[0-9]", "").alias("only_text"))
display(df_regexp_replace)

df_regexp_extract = df.select(col("address"),regexp_extract(col("address"), "([0-9]{6})", 1).alias("pincode"))
display(df_regexp_extract)


# COMMAND ----------

from pyspark.sql.functions import *

final_df = (
    df
    .select(
        col("emp_id"),

        # Clean & standardize name
        initcap(trim(col("emp_name"))).alias("emp_name"),

        # Normalize department
        upper(trim(col("department"))).alias("department"),

        # Email length validation
        length(col("email")).alias("email_length"),

        # Extract city & pincode
        split(col("address"), "-")[0].alias("city"),
        regexp_extract(col("address"), "([0-9]{6})", 1).alias("pincode"),

        # Human readable column
        concat_ws(
            " | ",
            initcap(trim(col("emp_name"))),
            upper(trim(col("department"))),
            col("email")
        ).alias("emp_profile")
    )
)

display(final_df)


# COMMAND ----------

#Problem-1
email_df = (
    df
    .select(
        col("emp_id"),
        col("email"),

        # Extract domain (gmail.com, yahoo.com, etc.)
        regexp_extract(col("email"), "@(.+)", 1).alias("email_domain"),

        # Email validity check
        when(col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"), "VALID")
        .otherwise("INVALID")
        .alias("email_status")
    )
)

display(email_df)


#Problem-2
address_df = (
    df
    .select(
        col("emp_id"),

        initcap(trim(split(col("address"), "-")[0])).alias("city"),

        regexp_extract(col("address"), "([0-9]{6})", 1).alias("pincode"),

        lit("INDIA").alias("country")
    )
)

display(address_df)


#Problem-3
dq_df = (
    df
    .select(
        col("emp_id"),

        # Name validation
        when(trim(col("emp_name")).isNull(), "MISSING")
        .when(length(trim(col("emp_name"))) < 3, "INVALID")
        .otherwise("VALID")
        .alias("emp_name_status"),

        # Email validation
        when(col("email").isNull(), "MISSING")
        .when(length(col("email")) < 10, "INVALID")
        .otherwise("VALID")
        .alias("email_status")
    )
)

display(dq_df)

#Problem-4
search_df = (
    df
    .select(
        col("emp_id"),

        lower(trim(col("emp_name"))).alias("emp_name_search"),

        lower(trim(col("department"))).alias("department_search"),

        lower(trim(split(col("address"), "-")[0])).alias("city_search")
    )
)

display(search_df)

#Problem-5
profile_df = (
    df
    .select(
        col("emp_id"),

        # Machine-friendly key
        concat_ws(
            "_",
            lower(trim(col("emp_name"))),
            lower(trim(split(col("address"), "-")[0]))
        ).alias("emp_key"),

        # Human-friendly display
        concat_ws(
            " | ",
            initcap(trim(col("emp_name"))),
            initcap(trim(split(col("address"), "-")[0])),
            col("email")
        ).alias("emp_display_profile")
    )
)

display(profile_df)

