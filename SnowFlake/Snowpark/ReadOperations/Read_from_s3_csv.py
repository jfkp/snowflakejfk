from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType,TimestampType,DoubleType

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

employee_s3 = session.read.csv('@my_s3_stage/employee/') # will fail because no schema

schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
 StructField("DOJ",DateType())])
# D:\training\SnowFlake\data\Employee\Employee
# Use session.read.schema and session.read.csv and mention the command to read data from s3
employee_s3 = session.read.schema(schema).csv('@my_s3_stage/employee/')
employee_s3.show()
employee_s3 = session.read.options({"ON_ERROR":"CONTINUE"}).schema(schema).csv('@my_s3_stage/employee/')
employee_s3.show()
type(employee_s3)

employee_s3 = employee_s3.cache_result() # to avoid creating temporary table 
# then select 
# then drop each time you call a show method
employee_s3.is_cached # return true if caching is ok

employee_s4=employee_s3.cache_result()

type(employee_s5)

employee_s3.columns

employee_s5=employee_s3.select("FIRST_NAME","LAST_NAME").filter(col("FIRST_NAME")=='Nyssa')
employee_s5.show()

employee_s3.show()

employee_s3.queries # will show al the query that will be generate on snowfklake side