-- Synapse Serverless SQL Views

-- Master Key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '...';

-- Credential
CREATE DATABASE SCOPED CREDENTIAL BlobCredential
WITH IDENTITY = 'Managed Identity';

-- External Data Source
CREATE EXTERNAL DATA SOURCE BlobStorage
WITH (
    LOCATION = 'https://nyctaxistoragead.blob.core.windows.net/raw-data',
    CREDENTIAL = SynapseIdentity
);





-- Clean View
CREATE VIEW dbo.yellow_taxi AS
SELECT *, '2015-01' AS data_period
FROM OPENROWSET(
    BULK 'yellow_tripdata_2015-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS taxi
UNION ALL
SELECT *, '2016-01' AS data_period
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS taxi
UNION ALL
SELECT *, '2016-02' AS data_period
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-02.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS taxi
UNION ALL
SELECT *, '2016-03' AS data_period
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-03.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS taxi;





-- Data Cleaning 

CREATE VIEW dbo.yellow_taxi_clean AS

SELECT
    CAST(VendorID AS INT)                                           AS VendorID,
    CAST(tpep_pickup_datetime AS DATETIME2)                         AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS DATETIME2)                        AS dropoff_datetime,
    CAST(passenger_count AS INT)                                    AS passenger_count,
    CAST(trip_distance AS FLOAT)                                    AS trip_distance,
    CAST(pickup_longitude AS FLOAT)                                 AS pickup_longitude,
    CAST(pickup_latitude AS FLOAT)                                  AS pickup_latitude,
    CAST(dropoff_longitude AS FLOAT)                                AS dropoff_longitude,
    CAST(dropoff_latitude AS FLOAT)                                 AS dropoff_latitude,
    CAST(payment_type AS INT)                                       AS payment_type,
    CAST(fare_amount AS FLOAT)                                      AS fare_amount,
    CAST(tip_amount AS FLOAT)                                       AS tip_amount,
    CAST(tolls_amount AS FLOAT)                                     AS tolls_amount,
    CAST(total_amount AS FLOAT)                                     AS total_amount,
    CAST(mta_tax AS FLOAT)                                          AS mta_tax,
    CAST(extra AS FLOAT)                                            AS extra,
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2))                   AS trip_year,
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2))                  AS trip_month,
    DAY(CAST(tpep_pickup_datetime AS DATETIME2))                    AS trip_day,
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2))         AS pickup_hour,
    DATEPART(WEEKDAY, CAST(tpep_pickup_datetime AS DATETIME2))      AS pickup_weekday,
    DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2))                   AS trip_duration_min,
    '2015-01'                                                       AS data_period
FROM OPENROWSET(
    BULK 'yellow_tripdata_2015-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE
    TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(fare_amount AS FLOAT) < 500
AND TRY_CAST(tip_amount AS FLOAT) >= 0
AND TRY_CAST(total_amount AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) < 100
AND TRY_CAST(passenger_count AS INT) > 0
AND TRY_CAST(passenger_count AS INT) <= 6
AND TRY_CAST(payment_type AS INT) IN (1,2,3,4,5,6)
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2015
AND CAST(tpep_dropoff_datetime AS DATETIME2) > CAST(tpep_pickup_datetime AS DATETIME2)
AND DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)) >= 1
AND DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)) <= 1440

UNION ALL

SELECT
    CAST(VendorID AS INT),
    CAST(tpep_pickup_datetime AS DATETIME2),
    CAST(tpep_dropoff_datetime AS DATETIME2),
    CAST(passenger_count AS INT),
    CAST(trip_distance AS FLOAT),
    CAST(pickup_longitude AS FLOAT),
    CAST(pickup_latitude AS FLOAT),
    CAST(dropoff_longitude AS FLOAT),
    CAST(dropoff_latitude AS FLOAT),
    CAST(payment_type AS INT),
    CAST(fare_amount AS FLOAT),
    CAST(tip_amount AS FLOAT),
    CAST(tolls_amount AS FLOAT),
    CAST(total_amount AS FLOAT),
    CAST(mta_tax AS FLOAT),
    CAST(extra AS FLOAT),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2)),
    DAY(CAST(tpep_pickup_datetime AS DATETIME2)),
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)),
    DATEPART(WEEKDAY, CAST(tpep_pickup_datetime AS DATETIME2)),
    DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)),
    '2016-01'
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE
    TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(fare_amount AS FLOAT) < 500
AND TRY_CAST(tip_amount AS FLOAT) >= 0
AND TRY_CAST(total_amount AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) < 100
AND TRY_CAST(passenger_count AS INT) > 0
AND TRY_CAST(passenger_count AS INT) <= 6
AND TRY_CAST(payment_type AS INT) IN (1,2,3,4,5,6)
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
AND CAST(tpep_dropoff_datetime AS DATETIME2) > CAST(tpep_pickup_datetime AS DATETIME2)
AND DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)) >= 1
AND DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)) <= 1440

UNION ALL

SELECT
    CAST(VendorID AS INT),
    CAST(tpep_pickup_datetime AS DATETIME2),
    CAST(tpep_dropoff_datetime AS DATETIME2),
    CAST(passenger_count AS INT),
    CAST(trip_distance AS FLOAT),
    CAST(pickup_longitude AS FLOAT),
    CAST(pickup_latitude AS FLOAT),
    CAST(dropoff_longitude AS FLOAT),
    CAST(dropoff_latitude AS FLOAT),
    CAST(payment_type AS INT),
    CAST(fare_amount AS FLOAT),
    CAST(tip_amount AS FLOAT),
    CAST(tolls_amount AS FLOAT),
    CAST(total_amount AS FLOAT),
    CAST(mta_tax AS FLOAT),
    CAST(extra AS FLOAT),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2)),
    DAY(CAST(tpep_pickup_datetime AS DATETIME2)),
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)),
    DATEPART(WEEKDAY, CAST(tpep_pickup_datetime AS DATETIME2)),
    DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)),
    '2016-02'
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-02.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE
    TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(fare_amount AS FLOAT) < 500
AND TRY_CAST(tip_amount AS FLOAT) >= 0
AND TRY_CAST(total_amount AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) < 100
AND TRY_CAST(passenger_count AS INT) > 0
AND TRY_CAST(passenger_count AS INT) <= 6
AND TRY_CAST(payment_type AS INT) IN (1,2,3,4,5,6)
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
AND CAST(tpep_dropoff_datetime AS DATETIME2) > CAST(tpep_pickup_datetime AS DATETIME2)
AND DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)) >= 1
AND DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)) <= 1440

UNION ALL

SELECT
    CAST(VendorID AS INT),
    CAST(tpep_pickup_datetime AS DATETIME2),
    CAST(tpep_dropoff_datetime AS DATETIME2),
    CAST(passenger_count AS INT),
    CAST(trip_distance AS FLOAT),
    CAST(pickup_longitude AS FLOAT),
    CAST(pickup_latitude AS FLOAT),
    CAST(dropoff_longitude AS FLOAT),
    CAST(dropoff_latitude AS FLOAT),
    CAST(payment_type AS INT),
    CAST(fare_amount AS FLOAT),
    CAST(tip_amount AS FLOAT),
    CAST(tolls_amount AS FLOAT),
    CAST(total_amount AS FLOAT),
    CAST(mta_tax AS FLOAT),
    CAST(extra AS FLOAT),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2)),
    DAY(CAST(tpep_pickup_datetime AS DATETIME2)),
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)),
    DATEPART(WEEKDAY, CAST(tpep_pickup_datetime AS DATETIME2)),
    DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)),
    '2016-03'
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-03.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(20)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE
    TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(fare_amount AS FLOAT) < 500
AND TRY_CAST(tip_amount AS FLOAT) >= 0
AND TRY_CAST(total_amount AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) < 100
AND TRY_CAST(passenger_count AS INT) > 0
AND TRY_CAST(passenger_count AS INT) <= 6
AND TRY_CAST(payment_type AS INT) IN (1,2,3,4,5,6)
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
AND CAST(tpep_dropoff_datetime AS DATETIME2) > CAST(tpep_pickup_datetime AS DATETIME2)
AND DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)) >= 1
AND DATEDIFF(MINUTE,
        CAST(tpep_pickup_datetime AS DATETIME2),
        CAST(tpep_dropoff_datetime AS DATETIME2)) <= 1440;



-- Verify the Clean Data Works
SELECT TOP 10 * FROM dbo.yellow_taxi_clean;


-- Aggregated Views

CREATE VIEW dbo.location_summary AS
SELECT 
    ROUND(TRY_CAST(pickup_longitude AS FLOAT), 3) AS pickup_longitude,
    ROUND(TRY_CAST(pickup_latitude AS FLOAT), 3)  AS pickup_latitude,
    COUNT(*)                                       AS total_trips,
    AVG(TRY_CAST(fare_amount AS FLOAT))            AS avg_fare,
    AVG(TRY_CAST(tip_amount AS FLOAT))             AS avg_tip,
    AVG(TRY_CAST(trip_distance AS FLOAT))          AS avg_distance
FROM OPENROWSET(
    BULK 'yellow_tripdata_2015-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE
    TRY_CAST(pickup_longitude AS FLOAT) BETWEEN -74.5 AND -73.5
AND TRY_CAST(pickup_latitude AS FLOAT) BETWEEN 40.4 AND 41.0
AND TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) > 0
AND TRY_CAST(passenger_count AS INT) > 0
GROUP BY
    ROUND(TRY_CAST(pickup_longitude AS FLOAT), 3),
    ROUND(TRY_CAST(pickup_latitude AS FLOAT), 3);



CREATE VIEW dbo.daily_trips AS
SELECT 
    CAST(tpep_pickup_datetime AS DATE)                          AS trip_date,
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2))               AS trip_year,
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2))              AS trip_month,
    COUNT(*)                                                    AS total_trips,
    AVG(TRY_CAST(fare_amount AS FLOAT))                         AS avg_fare,
    AVG(TRY_CAST(tip_amount AS FLOAT))                          AS avg_tip,
    AVG(TRY_CAST(trip_distance AS FLOAT))                       AS avg_distance,
    SUM(TRY_CAST(fare_amount AS FLOAT))                         AS total_fare
FROM OPENROWSET(
    BULK 'yellow_tripdata_2015-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) > 0
AND TRY_CAST(passenger_count AS INT) > 0
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2015
GROUP BY
    CAST(tpep_pickup_datetime AS DATE),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2))

UNION ALL

SELECT 
    CAST(tpep_pickup_datetime AS DATE),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2)),
    COUNT(*),
    AVG(TRY_CAST(fare_amount AS FLOAT)),
    AVG(TRY_CAST(tip_amount AS FLOAT)),
    AVG(TRY_CAST(trip_distance AS FLOAT)),
    SUM(TRY_CAST(fare_amount AS FLOAT))
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) > 0
AND TRY_CAST(passenger_count AS INT) > 0
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
GROUP BY
    CAST(tpep_pickup_datetime AS DATE),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2))

UNION ALL

SELECT 
    CAST(tpep_pickup_datetime AS DATE),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2)),
    COUNT(*),
    AVG(TRY_CAST(fare_amount AS FLOAT)),
    AVG(TRY_CAST(tip_amount AS FLOAT)),
    AVG(TRY_CAST(trip_distance AS FLOAT)),
    SUM(TRY_CAST(fare_amount AS FLOAT))
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-02.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) > 0
AND TRY_CAST(passenger_count AS INT) > 0
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
GROUP BY
    CAST(tpep_pickup_datetime AS DATE),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2))

UNION ALL

SELECT 
    CAST(tpep_pickup_datetime AS DATE),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2)),
    COUNT(*),
    AVG(TRY_CAST(fare_amount AS FLOAT)),
    AVG(TRY_CAST(tip_amount AS FLOAT)),
    AVG(TRY_CAST(trip_distance AS FLOAT)),
    SUM(TRY_CAST(fare_amount AS FLOAT))
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-03.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(trip_distance AS FLOAT) > 0
AND TRY_CAST(passenger_count AS INT) > 0
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
GROUP BY
    CAST(tpep_pickup_datetime AS DATE),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    MONTH(CAST(tpep_pickup_datetime AS DATETIME2));



CREATE VIEW dbo.hourly_fare AS
SELECT 
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)) AS pickup_hour,
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2))           AS trip_year,
    COUNT(*)                                                AS total_trips,
    AVG(TRY_CAST(fare_amount AS FLOAT))                     AS avg_fare,
    AVG(TRY_CAST(tip_amount AS FLOAT))                      AS avg_tip
FROM OPENROWSET(
    BULK 'yellow_tripdata_2015-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2015
GROUP BY
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2))

UNION ALL

SELECT 
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    COUNT(*),
    AVG(TRY_CAST(fare_amount AS FLOAT)),
    AVG(TRY_CAST(tip_amount AS FLOAT))
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
GROUP BY
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2))

UNION ALL

SELECT 
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    COUNT(*),
    AVG(TRY_CAST(fare_amount AS FLOAT)),
    AVG(TRY_CAST(tip_amount AS FLOAT))
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-02.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
GROUP BY
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2))

UNION ALL

SELECT 
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    COUNT(*),
    AVG(TRY_CAST(fare_amount AS FLOAT)),
    AVG(TRY_CAST(tip_amount AS FLOAT))
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-03.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
GROUP BY
    DATEPART(HOUR, CAST(tpep_pickup_datetime AS DATETIME2)),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2));




CREATE VIEW dbo.payment_summary AS
SELECT 
    TRY_CAST(payment_type AS INT)                           AS payment_type,
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2))           AS trip_year,
    COUNT(*)                                                AS total_trips,
    AVG(TRY_CAST(fare_amount AS FLOAT))                     AS avg_fare,
    AVG(TRY_CAST(tip_amount AS FLOAT))                      AS avg_tip
FROM OPENROWSET(
    BULK 'yellow_tripdata_2015-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(payment_type AS INT) IN (1,2,3,4,5,6)
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2015
GROUP BY
    TRY_CAST(payment_type AS INT),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2))

UNION ALL

SELECT 
    TRY_CAST(payment_type AS INT),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    COUNT(*),
    AVG(TRY_CAST(fare_amount AS FLOAT)),
    AVG(TRY_CAST(tip_amount AS FLOAT))
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-01.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(payment_type AS INT) IN (1,2,3,4,5,6)
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
GROUP BY
    TRY_CAST(payment_type AS INT),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2))

UNION ALL

SELECT 
    TRY_CAST(payment_type AS INT),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    COUNT(*),
    AVG(TRY_CAST(fare_amount AS FLOAT)),
    AVG(TRY_CAST(tip_amount AS FLOAT))
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-02.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(payment_type AS INT) IN (1,2,3,4,5,6)
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
GROUP BY
    TRY_CAST(payment_type AS INT),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2))

UNION ALL

SELECT 
    TRY_CAST(payment_type AS INT),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2)),
    COUNT(*),
    AVG(TRY_CAST(fare_amount AS FLOAT)),
    AVG(TRY_CAST(tip_amount AS FLOAT))
FROM OPENROWSET(
    BULK 'yellow_tripdata_2016-03.csv',
    DATA_SOURCE = 'BlobStorage',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    VendorID              VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_pickup_datetime  VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tpep_dropoff_datetime VARCHAR(30)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    passenger_count       VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    trip_distance         VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_longitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    pickup_latitude       VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    RatecodeID            VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    store_and_fwd_flag    VARCHAR(5)   COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_longitude     VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    dropoff_latitude      VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    payment_type          VARCHAR(10)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    fare_amount           VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    extra                 VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    mta_tax               VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tip_amount            VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    tolls_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    improvement_surcharge VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8,
    total_amount          VARCHAR(50)  COLLATE Latin1_General_100_CI_AS_SC_UTF8
) AS taxi
WHERE TRY_CAST(fare_amount AS FLOAT) > 0
AND TRY_CAST(payment_type AS INT) IN (1,2,3,4,5,6)
AND YEAR(CAST(tpep_pickup_datetime AS DATETIME2)) = 2016
GROUP BY
    TRY_CAST(payment_type AS INT),
    YEAR(CAST(tpep_pickup_datetime AS DATETIME2));



----verify
SELECT 'daily_trips' AS view_name, COUNT(*) AS rows FROM dbo.daily_trips
UNION ALL
SELECT 'hourly_fare', COUNT(*) FROM dbo.hourly_fare
UNION ALL
SELECT 'payment_summary', COUNT(*) FROM dbo.payment_summary
UNION ALL
SELECT 'location_summary', COUNT(*) FROM dbo.location_summary;

