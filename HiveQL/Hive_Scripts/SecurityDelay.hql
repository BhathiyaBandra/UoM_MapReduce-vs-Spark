DROP TABLE IF EXISTS DelayedFlights;

CREATE EXTERNAL TABLE DelayedFlights (
    ID int,
    Year int,
    Month int,
    DayofMonth int,
    DayOfWeek int,
    DepTime double,
    CRSDepTime double,
    ArrTime double,
    CRSArrTime double,
    UniqueCarrier string,
    FlightNum double,
    TailNum string,
    ActualElapsedTime double,
    CRSElapsedTime double,
    AirTime double,
    ArrDelay double,
    DepDelay double,
    Origin string,
    Dest string,
    Distance double,
    TaxiIn int,
    TaxiOut int,
    Cancelled int,
    CancellationCode string,
    Diverted double,
    CarrierDelay double,
    WeatherDelay double,
    NASDelay double,
    SecurityDelay double,
    LateAircraftDelay double
	)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	LINES TERMINATED BY '\n'
	STORED AS TEXTFILE
	LOCATION "${INPUT}";

 INSERT OVERWRITE DIRECTORY "${OUTPUT}"
 SELECT Year, AVG(SecurityDelay) FROM DelayedFlights GROUP BY Year;