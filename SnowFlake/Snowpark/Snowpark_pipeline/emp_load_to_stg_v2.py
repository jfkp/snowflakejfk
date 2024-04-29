import sys
sys.path.append('D:\\training\\SnowFlake\\Snowpark\\Snowpark_pipeline')
from generic_code import code_library
from schema import src_stg_schema
from snowflake.snowpark.context import get_active_session
import json

### Read from config file.
config_snow_copy = open('D:\\training\\SnowFlake\\Snowpark\\Snowpark_pipeline\\config\\copy_to_snowstg_avro.json', "r")
config_snow_copy = json.loads(config_snow_copy.read())

connection_parameter = open('D:\\training\\SnowFlake\\Snowpark\\Snowpark_pipeline\\config\\connection_details.json', "r")
connection_parameter = json.loads(connection_parameter.read())

session = code_library.snowconnection(connection_parameter)

df = session.read.avro("@my_s3_stage/Avro_folder/userdata1.avro")
df.printSchema()
df.columns()
copied_into_result, qid = copy_to_table_semi_struct_data(session,config_snow_copy,src_stg_schema.int_emp_details_avro)


print(copied_into_result)
print(qid)

copied_into_result_df = session.create_dataframe(copied_into_result)
copied_into_result_df.show()


f = open('D:\\training\\SnowFlake\\Snowpark\\Snowpark_pipeline\/config\\copy_to_snowflake.json', "r")
config = json.loads(f.read())




rejects.count()

get_active_session()

print(copied_into_result)

print(qid)

import pandas as pd
from snowflake.connector.options import installed_pandas, pandas

from snowflake.connector.options import installed_pandas

from snowflake.connector.options import installed_pandas

df = session.create_dataframe(pd.DataFrame([(1, 2, 3, 4)], columns=["a", "b", "c", "d"])).collect()

print(df)

df.show()

df = session.table("DEMO_DB.PUBLIC.SNOWPARK_TEMP_TABLE_GL8Z56B6A4")
#pip install "snowflake-connector-python[pandas]"

if  installed_pandas:
    print("hi")