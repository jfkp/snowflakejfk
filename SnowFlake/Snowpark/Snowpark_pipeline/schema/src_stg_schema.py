from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType,LongType,DoubleType

emp_stg_schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
StructField("DOJ",DateType())])

int_emp_details_avro = StructType([StructField('REGISTRATION_DTTM', StringType(), nullable=False), StructField('ID', LongType(), nullable=False), StructField('FIRST_NAME', StringType(), nullable=False), StructField('LAST_NAME', StringType(), nullable=False), StructField('EMAIL', StringType(), nullable=False), StructField('GENDER', StringType(), nullable=False), StructField('IP_ADDRESS', StringType(), nullable=False), StructField('CC', LongType(), nullable=True), StructField('COUNTRY', StringType(), nullable=False), StructField('BIRTHDATE', StringType(), nullable=False), StructField('SALARY', DoubleType(), nullable=True), StructField('TITLE', StringType(), nullable=False), StructField('COMMENTS', StringType(), nullable=False)])

emp_details_avro_cls = StructType([StructField('REGISTRTION', StringType(), nullable=False), \
    StructField('USER_ID', LongType(), nullable=False), \
        StructField('FIRST_NAME', StringType(), nullable=False), \
            StructField('LAST_NAME', StringType(), nullable=False), \
                StructField('USER_EMAIL', StringType(), nullable=False)])