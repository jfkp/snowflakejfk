# create table in snowflake
#create or replace TABLE EMPLOYEE 
#(FIRST_NAME VARCHAR(16777216),LAST_NAME VARCHAR(16777216),
# EMAIL VARCHAR(16777216),ADDRESS VARCHAR(16777216),
# CITY VARCHAR(16777216),    DOJ DATE);


from snowflake.snowpark import Session
from snowflake.snowpark import QueryRecord
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import substr

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"wkb48950.us-east-1",
"user":"jeanboscokpowadan",
"password": 'Tohono123&le',
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}
schema = StructType([StructField("FIRST_NAME", StringType()),
         StructField("LAST_NAME", StringType()),StructField("EMAIL", StringType()),
         StructField("ADDRESS", StringType())
        ,StructField("CITY", StringType()), StructField("DOJ",DateType())])

session = Session.builder.configs(connection_parameters).create()
employee_s3 = session.read.schema(schema).csv('@my_s3_stage/employee/')
copied_into_result=employee_s3.copy_into_table("employee",                                     
                                            target_columns=["FIRST_NAME","LAST_NAME","EMAIL","ADDRESS","CITY","DOJ"], 
                                            force=True,                                          
                                            on_error="CONTINUE")                                          

result_df = session.createDataFrame(copied_into_result)
result_df.show()
result_df.queries
                                           
# Technique to process reject records

with session.query_history() as query_history:
    copied_into_result=employee_s3.copy_into_table("employee",                                     
                                            target_columns=["FIRST_NAME","LAST_NAME","EMAIL","ADDRESS","CITY","DOJ"], 
                                            force=True,                                          
                                            on_error="CONTINUE"                                          
                                            )
query = query_history.queries

for id in query:
    if "COPY" in id.sql_text:
        qid = id.query_id
        rejects = session.sql("select *  from table(validate(employee , job_id =>'"+ "'"+ qid +"'))")
        rejects.show()

rejects = session.sql("select *  from table(validate(employee , job_id => '_last'))")
rejects.show()

# COPY DATA FROM LOCAL STORAGE TO SNOWFLAKE
putresult_list= session.file.put('D:\training\SnowFlake\data\Employee\Employee\employees01.csv','@my_s3_stage/employee/')