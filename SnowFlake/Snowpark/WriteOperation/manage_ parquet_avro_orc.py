from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType, TimestampType

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

schema = StructType([StructField("registration_dttm",TimestampType())
        ,StructField("id", IntegerType()),StructField("first_name",StringType()),
         StructField("last_name", StringType()),StructField("email", StringType()),
         StructField("gender",StringType()),StructField("ip_address",StringType()),
         StructField("cc",StringType()),StructField("birthdate",StringType()),
         StructField("salary",DoubleType()),StructField("title",StringType()),
         StructField("comments",StringType())])

employee_s3_parquet=session.read.parquet('@my_s3_stage/parquet_folder/')
employee_s3_parquet.select(col('"first_name"'),col('"last_name"'),col('"email"'),col('"gender"')).show()
employee_s3_parquet.write.mode("overwrite").save_as_table("DEMO_DB.PUBLIC.int_emp_details_parquet")

employee_s3_avro=session.read.avro('@my_s3_stage/avro_folder/')
emp_s3_avro_projection=employee_s3_avro.select(col('"first_name"'),col('"last_name"'),col('"email"'),col('"gender"'))
emp_s3_avro_projection.write.mode("overwrite").save_as_table("DEMO_DB.PUBLIC.user_emp_details_avro")
emp_s3_avro_projection.columns

emp_s3_orc=session.read.orc('@my_s3_stage/orc_folder/')
emp_s3_orc.columns
emp_s3_orc_projection=emp_s3_orc.select(col("$1").as_("id"),col("$2").as_("first_name"),col("$3").as_("last_name"),col("$4").as_("email"))

emp_s3_orc_projection=emp_s3_orc.with_column_renamed('"_col1"','id'). \
with_column_renamed('"_col2"','first_name'). \
with_column_renamed('"_col3"','last_name').with_column_renamed('"_col4"','email')
emp_s3_orc_projection.write.mode("overwrite").save_as_table("DEMO_DB.PUBLIC.user_emp_details_orc")

