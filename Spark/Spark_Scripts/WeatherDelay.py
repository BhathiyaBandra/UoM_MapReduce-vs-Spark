import sys
from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import avg, col
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType



conf = (SparkConf().setAppName("WeatherDelay"))
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

headers = ["ID","Year","Month","DayofMonth","DayOfWeek","DepTime","CRSDepTime","ArrTime","CRSArrTime","UniqueCarrier","FlightNum","TailNum","ActualElapsedTime","CRSElapsedTime","AirTime","ArrDelay","DepDelay","Origin","Dest","Distance","TaxiIn","TaxiOut","Cancelled","CancellationCode","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay"]

if __name__ == "__main__":
    inputFile = "s3://bhathiya/input/DelayedFlights-updated.csv"
    lines = sc.textFile(sys.argv[1])
    lines_nonempty = lines.filter( lambda x: len(x) > 0 )
    values = lines_nonempty.map(lambda line : line.split(","))
    df1 = spark.createDataFrame(values,headers)
    df2 = df1.withColumn("WeatherDelay",df1.WeatherDelay.cast('int'))
    df2.groupBy("Year").avg("WeatherDelay").show(truncate=False)
    sc.stop()
    spark.stop()
 
