import json 
import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df
from pandas import json_normalize

#load json object
with open('D:\\training\\SnowFlake\\Snowpark\\pythonprocedure\\raw_nyc_phil.json') as f:
    d = json.load(f)

#lets put the data into a pandas df
#clicking on raw_nyc_phil.json under "Input Files"
#tells us parent node is 'programs'
nycphil = json_normalize(d['programs'])
nycphil.head(3)

works_data = json_normalize(data=d['programs'], record_path=['works'],
                            meta=['id', 'orchestra','programID', 'season'])
works_data = works_data.drop(['soloists','movement.em','movement._','workTitle._','workTitle.em'], axis=1)

# Split conductor name as first-name and last-name
works_data[['composer_FirstName', 'composer_LastName']] = works_data['composerName'].loc[works_data['composerName'].str.split().str.len() == 2].str.split(expand=True)
works_data = works_data.drop(['composerName'], axis=1)
works_data.head(3)

# Create another data frame with name  soloist_df               
soloist_df = pd.json_normalize(data=d['programs'], record_path=['works', 'soloists'], 
                            meta=['id'])
                            
soloist_df[['soloist_FirstName', 'soloist_LastName']] = soloist_df['soloistName'].loc[soloist_df['soloistName'].str.split().str.len() == 2].str.split(expand=True)
soloist_df = soloist_df.drop(['soloistName'], axis=1)

soloist_df.head(3)