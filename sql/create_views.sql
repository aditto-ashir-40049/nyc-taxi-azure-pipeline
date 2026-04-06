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
CREATE VIEW dbo.yellow_taxi_clean AS ...

-- Aggregated Views
CREATE VIEW dbo.daily_trips AS ...
CREATE VIEW dbo.hourly_fare AS ...
CREATE VIEW dbo.payment_summary AS ...
CREATE VIEW dbo.location_summary AS ...