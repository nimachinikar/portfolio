from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import lit
from pyspark.sql.functions import when
from datetime import datetime




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

#What is the average price per night 
avgprc=listings_subset.select('IsDowntown','bedrooms','price').groupby('IsDowntown','bedrooms').agg(func.round(func.mean('price'),2).alias('price_avg'))
avgprc.show()

#Airbnb Revenue per month     
AirbnbDF=avg30.join(avgprc, (avg30.IsDowntown==avgprc.IsDowntown) & (avg30.bedrooms == avgprc.bedrooms),'inner').withColumn('Income_avg',(30-avg30.availability_30_avg)*avgprc.price_avg).select(avg30.IsDowntown,avg30.bedrooms,'availability_30_avg','price_avg','Income_avg')

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

#TODO CONTINUE
#regression?
#How can you make the most money?
#who is the richest Airbnb host in Vancouver? +investigation

#Review and income

#when can you charge the most and the less by neighbourhood? join calendar to list (to have neighbourhood)

#room_type

#instant_bookable


#out_path='C:/GitHub/portfolio/DataSet/project 3/subsetlistings'
#listings.repartition(1).write.option("header", "true").csv(out_path, mode = 'append')

