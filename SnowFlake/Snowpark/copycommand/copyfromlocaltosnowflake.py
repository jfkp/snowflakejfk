

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

schema = schema=StructType([StructField("name",datatype=StringType()),StructField("Address",datatype=StringType()),
                   StructField("City",datatype=StringType()),StructField("pin",datatype=IntegerType()),
                   StructField("Age",datatype=IntegerType())])

session = Session.builder.configs(connection_parameters).create()
session.sql("create or replace temp stage demo_db.public.i_my_stage").collect()
try:
    put_results=session.file.put('D:\\training\\SnowFlake\\data\\Employee\\Employee\\employee.csv','@i_my_stage',overwrite=True)
    for r in put_results:
        str_output = ("File {src}: {stat}").format(src=r.source,stat=r.status)
        print(str_output)   
except Exception as e:
    print(e)

employee_s3 = session.read.schema(schema).csv('@i_my_stage')
copied_into_result = employee_s3.copy_into_table("employee", target_columns=['FIRST_NAME','LAST_NAME','EMAIL','ADDRESS','CITY','DOJ'],on_error="CONTINUE")
rejects = session.sql("select *  from table(validate(employee , job_id => '_last'))")
rejects.show()
