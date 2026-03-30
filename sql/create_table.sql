CREATE TABLE dbo.yellow_taxi (
    VendorID            INT,
    pickup_datetime     DATETIME2,
    dropoff_datetime    DATETIME2,
    passenger_count     INT,
    trip_distance       FLOAT,
    RatecodeID          INT,
    PULocationID        INT,
    DOLocationID        INT,
    payment_type        INT,
    fare_amount         FLOAT,
    tip_amount          FLOAT,
    tolls_amount        FLOAT,
    total_amount        FLOAT
);
