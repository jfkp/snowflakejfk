import sys,os
from snowflake.snowpark import Session
sys.path.append('D:\\training\\SnowFlake\\Snowpark\\Snowpark_pipeline')
from generic_code import code_library
# Make connection and create Snowpark session
connection_parameters = {"account":"wkb48950.us-east-1",
"user":"jeanboscokpowadan",
"password": 'Tohono123&le',
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = code_library.snowconnection(connection_parameters)
session_new = code_library.snowconnection(connection_parameters)





put file:///Users/pradeep/Downloads/scrub/scrubadub/scrubbers.py @DEMO_DB.PUBLIC.UDF_STAGE/scrubadub/;