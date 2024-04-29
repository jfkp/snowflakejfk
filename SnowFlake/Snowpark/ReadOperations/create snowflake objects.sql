
create or replace storage integration s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::264444178311:role/snowflakeaccessrole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflakejfkinput/')
COMMENT = 'This an optional comment' ;
DESC integration s3_int;

create or replace file format my_csv_s3_format
    type = csv field_delimiter = ',' 
    skip_header = 1 
    null_if = ('NULL', 'null') 
    empty_field_as_null = true 
FIELD_OPTIONALLY_ENCLOSED_BY='"';

create or replace stage my_s3_stage 
url = 's3://snowflakejfkinput/'
STORAGE_INTEGRATION = s3_int
file_format = my_csv_s3_format;


grant usage on database demo_db to public;
grant usage on schema demo_db.public to public;
grant usage on stage my_s3_stage to public;

