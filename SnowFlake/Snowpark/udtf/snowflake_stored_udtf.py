import json
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import sproc,udtf,col
import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark.types 

from snowflake.snowpark.types  import PandasDataFrameType,MapType,PandasSeriesType,StringType,StructType,StructField,IntegerType,BooleanType

schema = StructType([
     StructField("soloistRoles", StringType()),
     StructField("soloistInstrument", StringType()),
     StructField("id", StringType()),
     StructField("soloist_FirstName", StringType()),
     StructField("soloist_LastName", StringType()),
 ])

@udtf(name="parse_json_sp_local_udtf", is_permanent=True, stage_location="@demo_stage", replace=True, packages=["snowflake-snowpark-python","pandas"],session=session_new,input_types=[StringType()],output_schema=schema)
class StockSale:
    def process(self,file_path):
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
        
            #session_new.write_pandas(soloist_df, "soloist_data_udf", auto_create_table=True,overwrite=True)
            #session_new.write_pandas(works_data, "programs_data_udf", auto_create_table=True,overwrite=True)

            for _, row in soloist_df.iterrows():
                yield  (row['soloistRoles'],row['soloistInstrument'],row['id'],row['soloist_FirstName'],
                row['soloist_LastName'])


##########################################################################################
#https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-functions
#https://docs.snowflake.com/en/sql-reference/functions-table
schema = StructType([
    StructField("symbol", StringType()),
    StructField("cost", StringType())
 ])

@udtf(name="process_stock_price", is_permanent=True, stage_location="@demo_stage", replace=True, packages=["snowflake-snowpark-python","pandas"],session=session_new,input_types=[StringType(),IntegerType(),IntegerType()],output_schema=schema)
class StockSale:
    def process(self, symbol, quantity, price):
         cost = quantity * price
         yield (symbol, cost)

##########################################################################################
#### Making UDTF partition aware
@udtf(name="process_stock_price_partition_aware", is_permanent=True, stage_location="@demo_stage", replace=True, packages=["snowflake-snowpark-python","pandas"],session=session_new,input_types=[StringType(),IntegerType(),IntegerType()],output_schema=schema)
class StockSaleSum:
  def __init__(self):
    self._cost_total = 0
    self._symbol = ""

  def process(self, symbol, quantity, price):
    self._symbol = symbol
    cost = quantity * price
    self._cost_total += cost
    #yield (symbol, cost)

  def end_partition(self):
    yield (self._symbol, self._cost_total)

##########################################################################################
schema = StructType([
     StructField("symbol", StringType())
 ])

@udtf(name="get_stock_data", is_permanent=True, stage_location="@demo_stage", replace=True, packages=["snowflake-snowpark-python","pandas"],session=session_new,input_types=[StringType()],output_schema=schema)
class StockSale:
    def process(self, symbol):
         session_new = snowpark.Session # is normally forbidden and should fail
         dataFrame = session_new.table("DEMO_DB.PUBLIC.CUSTOMER").filter(col("C_MKTSEGMENT") == symbol).collect()
         symbol = dataFrame
         yield (symbol)


schema = StructType([
     StructField("year", IntegerType()),
     StructField("color", StringType()),
     StructField("favorite", BooleanType())
 ])
@udtf(name="get_color", is_permanent=True, stage_location="@demo_stage", replace=True, packages=["snowflake-snowpark-python","pandas"],session=session_new,input_types=[IntegerType(),StringType(),BooleanType(),IntegerType()],output_schema=schema)
class Getcolor:
    def process(self,year,color,favorite,in_year):
         if in_year == year:
             yield(year,color,favorite)
         else:
             yield None
             
         #symbol = dataFrame

##########################################################################################     



        