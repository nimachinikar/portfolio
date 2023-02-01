from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, trim, when, lit, countDistinct

#import seaborn as sns 
#import matplotlib.pyplot as plt
#import pandas as pd



spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("VancouverAirbnb") \
    .getOrCreate()

listings = spark.read.option("header", "true").option("inferSchema", "true").csv("C:/GitHub/portfolio/DataSet/PySpark/listings.csv")
#http://insideairbnb.com/get-the-data/

#Is renting your condo on airbnb worth it?
# according to https://www.numbeo.com/cost-of-living/in/Vancouver
#TODO Webscrap this result
RefDF = spark.createDataFrame(
    [
         (True, 1,2451.00,datetime.today().strftime('%Y-%m-%d')),
         (True,3,4484.44,datetime.today().strftime('%Y-%m-%d')),
         (False,1,2076.92,datetime.today().strftime('%Y-%m-%d')),
         (False,3,3221.00,datetime.today().strftime('%Y-%m-%d'))
    ],
    ['IsDowntown', 'bedrooms', 'RentAvg','LookUpDate']
)

RefDF.show()
RefDF.printSchema()

#Exploratory Analysis
listings.printSchema()

#Removing $ sign in price and transforming to double
listings = listings.withColumn('price', func.regexp_replace('price', '[$,]', '').cast('double'))

#Filtering for odd prices
listings=listings.filter(listings.price<=20000)
#Manual check. Above 20k a night, it's data quality issue
oddlistings=listings.filter((listings.price>5000) & (listings.bedrooms <3))
#Manual check. Ideally I'd do a regression to estimate their price instead of removing them

#Removing the odd listings
listings=listings.join(oddlistings,listings.id==oddlistings.id,'leftanti')

#Since our comparison is only with 1bedroom and 3 bedrooms, lets narrow our data to that
listings_subset=listings.filter(listings.bedrooms.isin(1,3))

#Creating flag is in downtown as our reference value has that distinction
if 'IsDowntown' not in listings_subset.columns:
    listings_subset=listings_subset.withColumn('IsDowntown',\
                            when((listings_subset.neighbourhood_cleansed=='Downtown'),lit(True))\
                            .otherwise(lit(False))
                        )
#What is the average 30days availability
avg30=listings_subset.withColumn("availability_30",func.col("availability_30").cast('int')).select('IsDowntown','bedrooms','availability_30') \
         .groupby('IsDowntown','bedrooms').agg(func.round(func.mean('availability_30'),2).alias('availability_30_avg')).sort("availability_30_avg")
avg30.show()
###

#What is the average price per night 
avgprc=listings_subset.select('IsDowntown','bedrooms','price')\
                        .groupby('IsDowntown','bedrooms')\
                        .agg(func.round(func.mean('price'),2).alias('price_avg'))
avgprc.show()
###

#Airbnb Revenue per month     
AirbnbDF=avg30.join(avgprc, (avg30.IsDowntown==avgprc.IsDowntown) & (avg30.bedrooms == avgprc.bedrooms),'inner')\
                .withColumn('Income_avg',(30-avg30.availability_30_avg)*avgprc.price_avg)\
                .select(avg30.IsDowntown,avg30.bedrooms,'availability_30_avg','price_avg','Income_avg')

#Comparing Airbnb and Rental Revenues
ComparisonDF=AirbnbDF.join(RefDF, (AirbnbDF.IsDowntown==RefDF.IsDowntown) & (AirbnbDF.bedrooms == RefDF.bedrooms),'inner')\
                        .withColumn('IsAirbnbBetter', \
                            when ((AirbnbDF.Income_avg>RefDF.RentAvg),lit(True))\
                            .otherwise(lit(False))
                            )\
                        .select(AirbnbDF.IsDowntown,AirbnbDF.bedrooms,'Income_avg','RentAvg','IsAirbnbBetter')
ComparisonDF.show()
#At equal price between AirBNB and Renting, I decided that Renting is 'cheaper' as there is less cleaning fees involved with renting
#According to our data, it is always interesting to AirBNB #and that is without taking vacancy rate for rentals!
#taking into account vacancy by neighbourhood:  https://www03.cmhc-schl.gc.ca/hmip-pimh/en/TableMapChart/Table?TableId=2.1.31.3&GeographyId=2410&GeographyTypeId=3&DisplayAs=Table&GeograghyName=Vancouver#Total
#taxation different between airbnb and renting?
###

#Which is the most profitable property in Vancouver?
PropertyRevenueAirbnb=listings.withColumn('RevenuePerId',(365-listings.availability_365)*listings.price)
PropertyMasterAirbnb=PropertyRevenueAirbnb.select('id','host_id','availability_365','price','RevenuePerId','neighbourhood_cleansed','room_type','bedrooms','accommodates').orderBy(col('RevenuePerId').desc())
PropertyMasterAirbnb.show(1)
#It's a property rented $3500 a night with an availability_365 of 28 days only! It is an entire property in Kensington-Cedar Cottage which accommodates up to 8 guests. 

#Keeping only columns that matters to us
MasterAirbnb=PropertyMasterAirbnb.select('host_id','RevenuePerId')
#Grouping revenue by Host
MasterAirbnb=MasterAirbnb.groupby('host_id').agg(func.sum('RevenuePerId').alias('RevenuePerHost'),\
                                                 func.count('*').alias('NbProperties')\
                                                 ).sort(col("RevenuePerHost").desc())
MasterAirbnb.show(5)
#Host_id=231663454 . He/she made  $2,755,984 in 2022 with 75 properties listed in Vancouver! (81 in total)

####
#What impacts your income?
#Reviews?
print('Correlation between Revenue per property and reviews per months:' , PropertyRevenueAirbnb.stat.corr('RevenuePerId','reviews_per_month'))
#A higher amount of reviews is a sign of a poor accomodation. People are more likely to comment when it was negative experience

print('Correlation between Revenue per property and number of reviews:' , PropertyRevenueAirbnb.stat.corr('RevenuePerId','review_scores_rating'))
#Seems a lower value than expected. The data must be bias by accomodations which were rent for a small period of time and received very good reviews.
####

#What are the optimal neighbourhood, type of room and the number of accomodation to maximize your revenue?
AirbnbRevenueGrouped=PropertyMasterAirbnb.select('neighbourhood_cleansed','room_type','bedrooms','accommodates','RevenuePerId')\
                        .filter(PropertyMasterAirbnb.bedrooms.isNotNull())\
                        .groupBy('neighbourhood_cleansed','room_type','bedrooms','accommodates')\
                        .agg(func.sum('RevenuePerId').alias('RevenueGrouped'),\
                             func.count('*').alias('NbPropertiesPerCat'))
#Best neighbourhood for Income per head 
#RevenuePerHeadCat represents the sum of (non-availability of a property * price/night / accomodation ) / nb of properties in Cat (= neighboord, type of room and number of bedroom).
HighestRevenue=PropertyMasterAirbnb.select('neighbourhood_cleansed','RevenuePerId')\
                        .filter(PropertyMasterAirbnb.bedrooms.isNotNull())\
                        .groupBy('neighbourhood_cleansed')\
                        .agg(func.sum('RevenuePerId').alias('RevenueGrouped'),\
                             func.count('*').alias('NbPropertiesPerNeighboordhood'))
HighestRevenue.withColumn('Revenue',HighestRevenue.RevenueGrouped/HighestRevenue.NbPropertiesPerNeighboordhood).sort(col('Revenue').desc()).show()
#The Highest revenue is in Kitsilano, Arbutus Ridge and West Point Grey


#Best Configuration (nb of bedrooms and accomodation) for income per head
AirbnbRevenueGrouped=AirbnbRevenueGrouped.withColumn('RevenuePerHead_Cat',(AirbnbRevenueGrouped.RevenueGrouped/AirbnbRevenueGrouped.accommodates)/AirbnbRevenueGrouped.NbPropertiesPerCat)\
                        .orderBy(col('RevenuePerHead_Cat').desc())
#The best layout for maximizing your revenue is:
AirbnbRevenueGrouped.show(5)
#Oakridge, entire app/ home, 3 bedrooms, 5 accomodations with a total revenue of $88217.6 per year, per guest.
#The second best layout is to to rent an entire home/ apt of 3 bedrooms, 2 accommodates in Kitsilano
#Caution with this result, as the number of property per category is low and may bias the result

                        
#What is the best neighboord per configuration (nb of bedrooms and accomodation)
#Per entire unit
RevenueEntireHomeMax2PerRooms=AirbnbRevenueGrouped\
                        .filter(AirbnbRevenueGrouped.room_type=='Entire home/apt')\
                        .filter(AirbnbRevenueGrouped.accommodates<=AirbnbRevenueGrouped.bedrooms*2)\
                        .orderBy(col('bedrooms').asc(),col('accommodates').desc(),col('RevenuePerHead_Cat').desc(),col('room_type'),col('neighbourhood_cleansed'))

    #Creating top 3 condition
windowDept = Window.partitionBy('bedrooms','accommodates').orderBy(col('RevenuePerHead_Cat').desc())
RevenueEntireHomeMax2PerRooms=RevenueEntireHomeMax2PerRooms.withColumn('row',row_number().over(windowDept))

    #Top 3 by category                     
top3RevenueEntireHomeMax2PerRooms=RevenueEntireHomeMax2PerRooms.filter(col('row') <= 3)
top3RevenueEntireHomeMax2PerRooms.show()                
    #For Entire Home,
                    #1bedroom, 1accomodations in the order: DT, Grandview-Woodland, Kitsilano    
                    #1bedroom, 2accomodations             : West End, Stratacona, Dunbar Southlands
                    #2br,      4accomodations             : Stratacona,Kitsilano, Shaughnessy
                    #3br,      6accomodations             : South Cambie, Fairview, Dunbar Southlands

#Per Private room
RevenuePrivateRoomMax2PerRooms=AirbnbRevenueGrouped\
                        .filter(AirbnbRevenueGrouped.bedrooms.isNotNull())\
                        .filter(AirbnbRevenueGrouped.room_type=='Private room')\
                        .filter(AirbnbRevenueGrouped.accommodates<=AirbnbRevenueGrouped.bedrooms*2)\
                        .orderBy(col('bedrooms').asc(),col('accommodates').desc(),col('RevenuePerHead_Cat').desc(),col('room_type'),col('neighbourhood_cleansed'))
      
    #Creating top 3 condition      
RevenuePrivateRoomMax2PerRooms=RevenuePrivateRoomMax2PerRooms.withColumn('row',row_number().over(windowDept))
    #Top 3 by category 
top3RevenuePrivateRoomMax2PerRooms=RevenuePrivateRoomMax2PerRooms.filter(col('row') <= 3)
top3RevenuePrivateRoomMax2PerRooms.show()                 
    #For Private Room
                    #1bedroom, 1accomodations in the order: Strathcona,DT, Westpoint Grey, Dunbar Southlands       
                    #1bedroom, 2accomodations             : West End, Fairview, Shaughnessy
                    #2br,      4accomodations             : DT, Dunbar Southlands, Marpole
                    #3br,      6accomodations             : Dunbar Southlands, Marpole, Victoria-Fraserview
                    
#NOTE1: a large portion of this analysis was done on availability. I couldn't find documentation as what it exactly means. 
#For this exercises, I assumed that is a variable that shows the frequency of business of a property (how often it was/ wasnt booked)
#But it could be the frequency of availability to book the property (how often it is on the market to be booked).

#Availability 365 by neighbourhood
avg365_neighboordhood=listings.select('neighbourhood_cleansed','availability_365') \
         .groupby('neighbourhood_cleansed').agg(func.round(func.mean('availability_365'),2).alias('availability_365_avg')).sort(col('availability_365_avg').desc())

avg365_neighboordhood.show()         
#Conclusion
#It is always more interesting to Airbnb vs rent your place on AVERAGE
#The Highest revenue is in Kitsilano, Arbutus Ridge and West Point Grey
#The highest revenue per guest is not DT as well. It seems like the main reason for this is the difficulty to rent it. DT is most of the time available during the year!
#The highest revenue per guest is in  Oakridge and Kitsilano 
#The best Layout is: 3 bedrooms, 5 guest in Oakridge and 3 bedrooms, 2 guest in Kitsilano #Caution with this result
#For a one bedroom up to 2 guest, the best neighbourhood is in the West End for both private and Entire home.

    #Room for Improvement
        #Add vacancy rate cost for renting
        #Add taxation 
        #Add cleaning fees 
        #Cost of 'Welcoming guest'
        #Normalize data and remove extrem values
        #Add granularity (neighbourhood) for Airbnb vs Rental 
        #Have historic value to do a prediction on airbnb income per property
        #Confirm meaning of availability
    
