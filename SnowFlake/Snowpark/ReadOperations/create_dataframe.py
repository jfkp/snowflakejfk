# Databricks notebook source
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType
import time
# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"wkb48950.us-east-1",
"user":"jeanboscokpowadan",
"password": 'Tohono123&le',
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}
session = Session.builder.configs(connection_parameters).create()
session.sql("use warehouse compute_wh").collect()
# create a dataframe by inferring a schema from the data
test = session.create_dataframe([1, 2, 3, 4], schema=["a"])
test.show()
print(type(test))
#test = session.create_dataframe([[1, 2, 3, 123],[1, 2, 3, "ABC"],[1, 2, 3, "HPC"],[1, 2, 3, "EMD"]], schema=["a","b","c","d"])
#test.show()
test = session.create_dataframe([[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022']], schema=["a","b","c","d"])
test.show()
test = session.create_dataframe([[1, 2, 3, 26.897],[1, 2, 3, 27.897],[1, 2, 3, 29.897],[1, 2, 3, 39.897]], schema=["a","b","c","d"])
test.show(1)
test = session.create_dataframe([[1, 2, 3, None],[1, 2, 3, None],[1, 2, 3, None],[1, 2, 3, None]], schema=["a","b","c","d"])
test.show()
test = session.create_dataframe([[1, 2, 3, {"a":"hi"}],[1, 2, 3, None],[1, 2, 3, {"a":"Bye"}],[1, 2, 3, {"a":"hello"}]], schema=["a","b","c","d"])
test.show()
test = session.create_dataframe([[1, 2, 3, ["Hi"]],[1, 2, 3, None],[1, 2, 3,["Hello"] ],[1, 2, 3, ["Namaste"]]], schema=["a","b","c","d"])
test1 = test.cache_result()
test1.show()
print(type(test1))
# Check performance
begin = time.time()
test.show()
end = time.time()
print(f"Total runtime of the program is {end - begin}")
begin = time.time()
test1.show()
end = time.time()
print(f"Total runtime of the program is {end - begin}")
df_challenge=session.create_dataframe([[1, 2],[3,4],[None,5]], schema=["a","b"])
df_challenge.show()
schema = StructType([StructField("Name", StringType()),
         StructField("Salary",  IntegerType()),
         StructField("Doj",  DateType())])
df_challenge=session.create_dataframe([["John", "100","2016-01-01"],["Sam", "200","2017-01-01"]], schema=schema)
df_challenge.show()
df_orders_info=session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS")
df_orders_select=df_orders_info.select(col("O_ORDERKEY"),col("O_ORDERSTATUS"),col("O_TOTALPRICE"))
df_orders_select.describe().sort("SUMMARY").show()