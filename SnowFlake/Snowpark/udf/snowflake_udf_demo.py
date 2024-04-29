# Creating snowpark python UDF

# Create or replace stage demo_db.public.udf_stage;

from snowflake.snowpark.types import IntegerType, StringType
from snowflake.snowpark.functions import udf, col
import pandas as pd
import sys
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
session_new = code_library.snowconnection(connection_parameters)

@udf(session = session_new,name='a_plus_b', input_types=[IntegerType(), IntegerType()], return_type=IntegerType(), stage_location='@udf_stage',is_permanent=True, replace=True)
def a_plus_b(a: int, b: int) -> int:
    return a + b

df = session_new.create_dataframe(pd.DataFrame([(1, 2, 3, 4)], columns=["a", "b", "c", "d"]))
df.show()
df.withColumn('A_PLUS_B', a_plus_b(col('"a"'), col('"b"'))).show()

df = session_new.table("DEMO_DB.PUBLIC.CUSTOMER_TEST")
dk = df.withColumn('A_PLUS_B', a_plus_b(col('C_CUSTOMER_SK'))).show()

###### Using python packages #######

session_new.add_packages("email_validator")

@udf(session = session_new,name='email_validate', input_types=[StringType()], return_type=StringType(), stage_location='@udf_stage',is_permanent=True, replace=True)
def email_validate(email: str) -> str:
    from email_validator import validate_email, EmailNotValidError
    try:
        emailinfo = validate_email(email, check_deliverability=False)
        return "VALID"
    except EmailNotValidError as e:
        return "NOTVALID"
    

##### Vectorised UDF #####
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType, PandasSeriesType, PandasDataFrameType

@udf(session = session_new,name='a_plus_b_vector', max_batch_size=200,input_types=[PandasDataFrameType([IntegerType(), IntegerType()])], return_type=PandasSeriesType(IntegerType()), stage_location='@udf_stage',is_permanent=True, replace=True)
def a_plus_b_vector(df):
    return df[0]+df[1]


######### Snowflake external packages ###########
from snowflake.snowpark.functions import udf

session_new.add_packages("numpy", "pandas","catalogue","faker","phonenumbers","dateparser","nltk","textblob")
#session.add_import("@DEMO_DB.PUBLIC.UDF_STAGE/scrubadub/scrubadub.zip")
session_new.add_import('D:\\training\\SnowFlake\\Snowpark\\udf\\python_packages\\scrub\\scrubadubw\\scrubadub')
session_new.add_import('D:\\training\\SnowFlake\\Snowpark\\udf\\python_packages\\python-stdnum-1.18\\stdnum')
session_new.add_import('D:\\training\\SnowFlake\\Snowpark\\udf\\python_packages\\textblob-0.17.1\\textblob')
session_new.add_import('D:\\training\\SnowFlake\\Snowpark\\udf\\python_packages\\xlrd-2.0.1\\xlrd')

@udf(session=session_new ,name="external_scrub_text", is_permanent=True, stage_location="@UDF_STAGE", replace=True)
def scrub_text(x: str) -> str:
    import scrubadub as sc
    txt = sc.clean(x)
    return txt