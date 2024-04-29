import json
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import sproc
import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import BooleanType,PandasDataFrameType,MapType,PandasSeriesType,StringType

@udf(name="parse_json_sp_local_udf", is_permanent=True, stage_location="@demo_stage", replace=True, packages=["snowflake-snowpark-python","pandas"],session=session_new,input_types=[StringType()],return_type=MapType())
def main(file_path):
 with SnowflakeFile.open(file_path) as f:
     # Read json file and normalize data.
     nycphil = json.load(f)
     works_data = pd.json_normalize(data=nycphil['programs'], record_path='works', meta=['id', 'orchestra','programID','season'])
     # Drop columns which is not required.
     works_data = works_data.drop(['soloists','movement.em','movement._','workTitle._','workTitle.em'], axis=1)

     
     # Split conductor name as first-name and last-name
     works_data[['composer_FirstName', 'composer_LastName']] = works_data['composerName'].loc[works_data['composerName'].str.split().str.len() == 2].str.split(expand=True)
     works_data = works_data.drop(['composerName'], axis=1)
     
     # Create another data frame with name  soloist_df               
     soloist_df = pd.json_normalize(data=nycphil['programs'], record_path=['works', 'soloists'], 
                            meta=['id'])
                            
     soloist_df[['soloist_FirstName', 'soloist_LastName']] = soloist_df['soloistName'].loc[soloist_df['soloistName'].str.split().str.len() == 2].str.split(expand=True)
     soloist_df = soloist_df.drop(['soloistName'], axis=1)
     # udf does not allow to write dataframe to table
     #session_new.write_pandas(soloist_df, "soloist_data_udf", auto_create_table=True,overwrite=True)
     #session_new.write_pandas(works_data, "programs_data_udf", auto_create_table=True,overwrite=True)
 # udf must return values stored proc dont have to
 return soloist_df.to_dict()

# UDF MUST BE CALLED AS PART OF SQL STATMENT