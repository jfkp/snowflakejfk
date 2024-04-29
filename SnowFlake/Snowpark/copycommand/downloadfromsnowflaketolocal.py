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
emp_df = session.sql("select * from demo_db.public.employee")
result=emp_df.write.copy_into_location('@i_my_stage',file_format_type="csv",header=True,overwrite=True, \
                                       format_type_options=({"field_optionally_enclosed_by":'"'}))
session.file.get('@i_my_stage','D:\\training\\SnowFlake\\data\\Employee\\Employee\\')


# une autre facon de faire

# Create a temp stage.
_ = session.sql("create or replace temp stage demo_db.public.mystage").collect()

# Unload data from snowflake table employee to stage locaion @mystage/download/
emp_stg_tbl = session.table("DEMO_DB.PUBLIC.EMPLOYEE")
copy_result = emp_stg_tbl.write.copy_into_location('@mystage/download/', file_format_type="csv", header=True, overwrite=True, single=True)


# Download files from internal stage to your local path
get_result1 = session.file.get("@myStage/download/", "data/downloaded/emp/")