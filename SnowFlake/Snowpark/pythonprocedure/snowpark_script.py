
#CREATE OR REPLACE STAGE DEMO_STAGE;
#IN SNOWSQL EXECUTE THE FOLLOWING COMMAND
#PUT file:///D:\\training\\SnowFlake\\Snowpark\\pythonprocedure\\raw_nyc_phil.json @DEMO_STAGE AUTO_COMPRESS=FALSE;
#PUT file:///D:\\training\\SnowFlake\\Snowpark\\pythonprocedure\\sample_file.xlsx @DEMO_STAGE AUTO_COMPRESS=FALSE;

IN SNOWFLAKE WORKSHEET

CREATE OR REPLACE PROCEDURE DEMO_DB.PUBLIC.PARSE_JSON_SP("FILE_PATH" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import json
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd

def main(session, file_path):
 with SnowflakeFile.open(file_path, ''rb'') as f:
     # Read json file and normalize data.
     nycphil = json.load(f)
     works_data = pd.json_normalize(data=nycphil[''programs''], record_path=''works'', 
                            meta=[''id'', ''orchestra'',''programID'', ''season''])
     # Drop columns which is not required.
     works_data = works_data.drop([''soloists'',''movement.em'',''movement._'',''workTitle._'',''workTitle.em''], axis=1)

     
     # Split conductor name as first-name and last-name
     works_data[[''composer_FirstName'', ''composer_LastName'']] = works_data[''composerName''].loc[works_data[''composerName''].str.split().str.len() == 2].str.split(expand=True)
     works_data = works_data.drop([''composerName''], axis=1)
     
     # Create another data frame with name  soloist_df               
     soloist_df = pd.json_normalize(data=nycphil[''programs''], record_path=[''works'', ''soloists''], 
                            meta=[''id''])
                            
     soloist_df[[''soloist_FirstName'', ''soloist_LastName'']] = soloist_df[''soloistName''].loc[soloist_df[''soloistName''].str.split().str.len() == 2].str.split(expand=True)
     soloist_df = soloist_df.drop([''soloistName''], axis=1)
 
     session.write_pandas(soloist_df, "soloist_data", auto_create_table=True,overwrite=True)
     session.write_pandas(works_data, "programs_data", auto_create_table=True,overwrite=True)
 
 return True
';
# to Call the procedure in worksheet
call PARSE_JSON_SP(build_scoped_file_url('@demo_stage','raw_nyc_phil.json'))