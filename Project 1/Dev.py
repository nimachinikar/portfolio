import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('whitegrid')


#Displaying all columns
pd.options.display.max_columns = None
pd.options.display.max_rows = None


# Print the current working directory
print("Current working directory: {0}".format(os.getcwd()))

# Change the current working directory
os.chdir('C:\GitHub\portfolio\DataSet')

# Print the current working directory
print("Current working directory: {0}".format(os.getcwd()))

#import Dataset
data = pd.read_csv('SGO-2021-01_Incident_Reports_ADS.csv')

## Data Exploration
#overview Dataset
data.head(5)


#Check DataType
data.info(verbose=True, show_counts=True)

#Size of DataSet
data.shape

#INVESTIGATING BAD DATA
#1) Missing Value
#496 rows, 122 columns
#Check Missing Data
data.isnull().sum().sort_values(ascending=0)
#Report Month and Report Year also have 247 rows missing, we will populate them later

#Weather - Other text is always blank, Serial number, investigation officer email, source - other, investigation officer phone are up to 93% blank
# name / agency have more than 352/496 empty rows blank and are not information we need


#I didnt want to copy past the name of each column. So I did the top 7
#But the output is a serie. So it was transformed to a DF and I extracted the column having the columns' name
data = data.drop(data.isnull().sum().sort_values(ascending=0).head(7).to_frame(name="columns").axes[0].tolist(), axis = 1)

#2) Unusable data and GDPR Reg
#Counting columns having PII data
#Creating the table containing the count of PII data per columns
PII_tbl=pd.DataFrame()

#Creating a loop to go through each column
for column in list(data.select_dtypes('object').columns.to_list()):

  tbl=pd.DataFrame([data[column].str.contains('PERSONALLY IDENTIFIABLE INFORMATION').value_counts()])

# Adding True or False for each row
  for i in ('True','False'):
    tbl[i] = tbl.get(i,0)

# Creating a nbTrue and False
  if True in tbl.columns:
    tbl["NbTrue"] = tbl[True]
  else:
    tbl["NbTrue"] = tbl['True']

  tbl=tbl[['NbTrue']]

  # Assemble loop result
  PII_tbl= pd.concat([PII_tbl, tbl])

# Columns with PII info (sorted)
PII_tbl=PII_tbl.loc[(PII_tbl!=0).any(axis=1)].sort_values(by = ['NbTrue'],  ascending=False)

#Address, Zip code, Latitude and Longitude have a large majority of PII. Let's drop them
col_to_remove = {}
for row in PII_tbl.index:
   col_to_remove[row] = print(row, end = ", ")
col_to_remove=list(col_to_remove.keys())

for col in col_to_remove:
 data.drop([col], inplace=True,  axis=1)

# Check new Size of DataSet
 data.shape

#There a several version per report
data['Report ID'].nunique() == len(data)

# Let's keep the max Report Version per Report Id
data=data.loc[data.groupby(["Report ID"])["Report Version"].idxmax()]

##Missing Values
# Populating value in Report Month and Report Year from Report Submission Date
 data.dtypes
 #Report Submission Date, Incident Date are not a date

 #Creating function for transforming string to date
 def fct_DateFormat(df,col):
     df[col] = pd.to_datetime(df[col].apply(lambda x: dt.strptime(x, '%b-%y')))

fct_DateFormat(data,'Incident Date')
fct_DateFormat(data, 'Report Submission Date')

#Subscracting year/ month from Report Submission Date But keeping it in seperate column to verify value
 data['Report Year2']=data['Report Submission Date'].dt.year
 data['Report Month2']=data['Report Submission Date'].dt.month

no_match=len( data[data['Report Year2']!= data['Report Year']][["Report Year","Report Year2","Report Month",'Report Submission Date']].dropna())
match=len(data[data['Report Year2'] == data['Report Year']][["Report Year", "Report Year2", "Report Month", 'Report Submission Date']].dropna())
ratio_match_nomatch= round(no_match/match*100)
#6% doesnt match

 #Lets look at those 6%
 data[data['Report Year2']!= data['Report Year']][["Report Year","Report Year2","Report Month",'Report Submission Date']].dropna()
 #except for one it it's always dec-jan
 # We can conclude that the year in Report Submission Date can be a proxy for Report Year

 #let's confirm with the same study with month
no_match=len( data[data['Report Month2']!= data['Report Month']][["Report Month","Report Month2","Report Year",'Report Submission Date']].dropna())
match=len(data[data['Report Month2'] == data['Report Month']][["Report Month", "Report Month2", "Report Year", 'Report Submission Date']].dropna())
ratio_match_nomatch= round(no_match/match*100)

#We can see with no_match and match that data is more volatile here.
# We should be using regression to find the appropriate proxy
#However since we have complete information in Report Submission Date, I suggest to drop the year and month from the original table.
data.drop(['Report Year', 'Report Month'], axis = 1, inplace = True)

# the year and month part from Report Submission Date will replace them.
data['Report Submission Year'] = data['Report Year2']
data['Report Submission Month'] = data['Report Month2']

data.drop(['Report Year2', 'Report Month2'], axis = 1, inplace = True)

#Check Blank Data
DataBlank = []
#Creating a loop to go through each column
for column in list(data.select_dtypes('object').columns.to_list()):
    value= (data[column]== ' ').sum()
    DataBlank.append((column, value))

DataBlank=pd.DataFrame.from_dict(DataBlank).rename(columns = {0:'Column', 1:'NbBlank'}).astype({'NbBlank':'int'}).sort_values(by = ['NbBlank'],  ascending=False)
DataBlank=DataBlank[DataBlank['NbBlank'] > 100]
DataBlank_original=DataBlank
#Transposing the variable(rows) to columns
DataBlankUnique = []
for row in DataBlank['Column']:
    DataBlankUnique.append((row, data[row].unique()))
DataBlankUnique=pd.DataFrame.from_dict(DataBlankUnique).rename(columns = {0:'Column', 1:'BlankDistinctValues'})

DataBlank = pd.merge(DataBlank,DataBlankUnique, how='inner',on='Column')
# The values are either null or yes. Let's remove column with 'unknown' as this is a duplicate information from other columns or not even populated info

#Removing Unknown column
DataBlank_not_unknown=DataBlank[~DataBlank['Column'].str.contains('Unknown')][['Column']]
DataBlank=pd.merge(DataBlank, DataBlank_not_unknown, how='inner')

#lets remove column having only blank value
OnlyBlank=DataBlank[DataBlank['NbBlank']!=len(data)][['Column']]
DataBlank=pd.merge(DataBlank, OnlyBlank, how='inner')

#They are still a lot of blank value, but at this stage I dont want to take the risk of loosing an important info
#filter data with remaining columns

#AntiJoin to get the columns we removed in DataBlank
ColumnsToDrop = DataBlank_original.merge(DataBlank, how = 'outer', indicator = True)
ColumnsToDrop = ColumnsToDrop[~(ColumnsToDrop._merge == 'both')].drop('_merge', axis = 1)

#Removing columns in data
col_to_remove = {}
for row in ColumnsToDrop['Column']:
   col_to_remove[row] = print(row, end = ", ")
col_to_remove=list(col_to_remove.keys())

for col in col_to_remove:
 data.drop([col], inplace=True,  axis=1)

# Check new Size of DataSet
 data.shape




#
list(data.columns.to_list())
VIN
Make
Model
Model Year
Incident Time
City
'Roadway Type', 'Roadway Surface', 'Roadway Description',
Posted Speed Limit (MPH)', 'Lighting'
'CP Any Air Bags Deployed?', 'CP Was Vehicle Towed?'

#What is the brand with the most accident
plot=sns.countplot(x='Make',data=data,palette='viridis',order = data['Make'].value_counts().index)
plt.setp(plot.get_xticklabels(), rotation=90)
plt.show()

#When do most of accidents occur?
byMonth = data.groupby('Report Submission Month').count()
byMonth['Report ID'].plot()
plt.show()
#September seems to be the worst frequent time

#How about Lightning
plt=sns.countplot(x='Lighting',data=data.sort_values('Incident Time (24:00)'),hue='Reason',palette='viridis')
plt.show()
