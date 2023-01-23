import requests
from bs4 import BeautifulSoup as soup
import pandas as pd
import numpy as np
header = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    'referer': 'https://www.zillow.com/homes/for_sale/Vancouver,-BC_rb/?searchQueryState=%7B%22pagination'

    }

url = 'https://www.zillow.com/homes/for_sale/Vancouver,-BC_rb/'
html = requests.get(url=url, headers=header)

#pip install lxml
bsobj=soup(html.content,'lxml')

bsobj

info=[]
for i in bsobj.findAll('div',{'class':'StyledPropertyCardDataWrapper-c11n-8-81-1__sc-1omp4c3-0 fEStTH property-card-data'}):
   info.append(i.text.replace('$','|').replace('MLS® ID #','|').replace('bds','bds|').replace('1 bd','1 bd|').replace(',000','000|').replace(',999','999|').replace(',','|').replace(' -','|').strip().split('|')[:-1])
   #TODO Make it more robust
   #two issues to solve later
   #condo/ house over a million are split in two columns
   #if sqf condo >999 then the thousand is in the former last element
   #if sqf<=999 then it is attached to the bath info and needs to be split

df = pd.DataFrame(info,columns=['Address','City','Postal Code','MLS ID','Broker','price1','price2','NbBeds','NbBath','SQFT'])

#Solving sqf
#Bringing data in sqft
df['SQFT'] = df[['SQFT', 'NbBath', 'NbBeds']].bfill(axis=1).iloc[:, 0]
df['NbBath'] = df[['NbBath', 'NbBeds']].bfill(axis=1).iloc[:, 0]

#SQFT
nb=('0','1','2','3','4','5','6','7','8','9')
#Extracting last value in nb bath into a new column. Adding that value if necessary to SQFT
def SQFT_cleaning(bath_col):
    #extracting last element of bath_col if it's in nb and saving it in a new column called sqft_unit1
    df.loc[df[bath_col].str.strip().str[-1:].isin(nb), 'SQFT_unit1'] = df[bath_col].str.strip().str[-1:]
        #replace nan with 0 and merging sqft_unit1 and sqft
         #that is to bring the missing part of sqft
    df['SQFT']=df['SQFT_unit1'].replace(np.nan, 0).astype(str) +""+ df["SQFT"]
        #Replacing ba with | to split the bathroom and sqft info
    df['SQFT'] = df['SQFT'].str.replace('ba','|')
    df[['SQFT','SQFT_unit1']] = df['SQFT'].str.split('|',expand=True) #TODO Modified to accomodate second run #np.where(df3['SQFT'].str.contains('|', regex=False),df3['SQFT'].str.split('|', expand=True), (df3['SQFT']+ '|').str.split('|', expand=True))
        #Coalesce
    df['SQFT'] = df[['SQFT_unit1', 'SQFT']].bfill(axis=1).iloc[:, 0]
        #dropping SQFT_unit1

SQFT_cleaning('NbBath')
df=df.drop(columns=['SQFT_unit1'], axis=1)


#There could be more than 1 digit (10000+ sqft). so we will reproduce the previous step UNDER DEV
#Removing 'sqft' and making an int
df['SQFT']=df['SQFT'].str.rstrip('sqft').astype(int)

def Bath_Info(col1,col_before_col1,devcol1='delete_me'):
    #Gathering Nb Bath info from Nb Beds when necessary
    df[devcol1]=df[~df[col1].str.contains('ba')][col_before_col1]
    #coalesce
    df[col1] = df[[devcol1, col1]].bfill(axis=1).iloc[:, 0]
    #replacing ba with | and then split
    df[col1]=df[col1].str.replace('ba','|')
    df[[col1,devcol1]] = df[col1].str.split('|',expand=True)
    #Making an int
    df[col1]=df[col1].astype(int)

Bath_Info('NbBath','NbBeds')

def Bed_Info(col1,col_before_col1,devcol1='delete_me'):
    #Gathering Nb Beds info from price2 when necessary
    df[devcol1]=df[df[col1].str.contains('ba')][col_before_col1]
    #coalesce
    df[col1] = df[[devcol1, col1]].bfill(axis=1).iloc[:, 0]
    #replacing bd with | and then split
    df[col1]=df[col1].str.replace('bd','|')
    df[[col1,devcol1]] = df[col1].str.split('|',expand=True)
    #Making an int
    df[col1]=df[col1].astype(int)

Bed_Info('NbBeds','price2')
def Price_Info():
    #Gathering price2 info from price1 when necessary
    df['delete_me']=df[~df['price2'].str.contains('bd')]['price2']
    #Merge 2 prices
    df['delete_me']=df['price1'].astype(str) +""+ df["delete_me"]
    #Filling null with price1
    df['delete_me']=df['delete_me'].fillna(df.price1)
    #Creating Price Column
    df['Price']=df['delete_me'].astype(int)

df=df.drop(columns=['delete_me','price1','price2'], axis=1)