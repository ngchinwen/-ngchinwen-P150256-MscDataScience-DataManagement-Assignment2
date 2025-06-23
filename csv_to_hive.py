from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV to Hive") \
    .enableHiveSupport() \
    .getOrCreate()

## read all csv files
df_car = spark.read.option("header", "true").csv("hdfs:///user/maria_dev/carriers.csv")
df_air = spark.read.option("header", "true").csv("hdfs:///user/maria_dev/airports.csv")
df_plane = spark.read.option("header","true").csv("hdfs:///user/maria_dev/plane-data.csv")
df_2007 = spark.read.option("header","true").csv("hdfs:///user/maria_dev/2007.csv")

#change data type
ccol = ["Year", "Month", "DayofMonth","DayOfWeek","DepTime","CRSDepTime","ArrTime","CRSArrTime","ActualElapsedTime","CRSElapsedTime","AirTime","ArrDelay", "DepDelay", "Distance","TaxiIn","TaxiOut","Cancelled","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay"]

for col_name in ccol:
    df_2007 = df_2007.withColumn(col_name, col(col_name).cast(IntegerType()))

## write them into hive tables
df_car.write.mode("overwrite").saveAsTable("carriers")
df_air.write.mode("overwrite").saveAsTable("airports")
df_plane.write.mode("overwrite").saveAsTable("plane_data")
df_2007.write.mode("overwrite").saveAsTable("year_2007")
