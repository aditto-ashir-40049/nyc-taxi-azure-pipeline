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
