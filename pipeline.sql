CREATE USER [bohdan_z] FOR LOGIN [BohdanZhuravel] WITH DEFAULT_SCHEMA=[zhuravel_schema]
GO


CREATE MASTER KEY;
GO

CREATE DATABASE SCOPED CREDENTIAL BohdanZhuravelDataFromBlob
WITH 
    IDENTITY = 'NotApplicable',
    SECRET = 'SECRET'
;
GO

CREATE EXTERNAL DATA SOURCE BohdanZhuravelSource
WITH
  ( LOCATION = 'wasbs://container1@BLOB' ,
    CREDENTIAL = BohdanZhuravelDataFromBlob,
    TYPE = HADOOP
);
GO

CREATE EXTERNAL FILE FORMAT CSVFILE
WITH (
    FORMAT_TYPE = DelimitedText,
    FORMAT_OPTIONS (FIELD_TERMINATOR = ',')

);
GO

CREATE EXTERNAL TABLE zhuravel_schema.yellow_trip_external (
  [VendorID] [int] NULL,
  [tpep_pickup_datetime] [datetime] NOT NULL,
  [tpep_dropoff_datetime] [datetime] NOT NULL,
  [passenger_count] [int] NULL,
  [trip_distance] [float] NOT NULL,
  [RatecodeID] [int] NULL,
  [store_and_fwd_flag] [varchar](1) NULL,
  [PULocationID] [int] NOT NULL,
  [DOLocationID] [int] NOT NULL,
  [payment_type] [int] NULL,
  [fare_amount] [float] NOT NULL,
  [extra] [float] NOT NULL,
  [mta_tax] [float] NOT NULL,
  [tip_amount] [float] NOT NULL,
  [tolls_amount] [float] NOT NULL,
  [improvement_surcharge] [float] NOT NULL,
  [total_amount] [float] NOT NULL,
  [congestion_surcharge] [float] NOT NULL
)
WITH (
        LOCATION='/yellow_tripdata_2020-01.csv',
        DATA_SOURCE = BohdanZhuravelSource,
        FILE_FORMAT = CSVFILE
    )
;

CREATE TABLE zhuravel_schema.fact_tripdata
(
	[VendorID] [int] NULL,
	[tpep_pickup_datetime] [datetime] NOT NULL,
	[tpep_dropoff_datetime] [datetime] NOT NULL,
	[passenger_count] [int] NULL,
	[trip_distance] [float] NOT NULL,
	[RatecodeID] [int] NULL,
	[store_and_fwd_flag] [varchar](1) NULL,
	[PULocationID] [int] NOT NULL,
	[DOLocationID] [int] NOT NULL,
	[payment_type] [int] NULL,
	[fare_amount] [float] NOT NULL,
	[extra] [float] NOT NULL,
	[mta_tax] [float] NOT NULL,
	[tip_amount] [float] NOT NULL,
	[tolls_amount] [float] NOT NULL,
	[improvement_surcharge] [float] NOT NULL,
	[total_amount] [float] NOT NULL,
	[congestion_surcharge] [float] NOT NULL
)
WITH
(
	DISTRIBUTION = HASH ( [tpep_pickup_datetime] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [zhuravel_schema].[Payment_type]
(
	[ID] [int] NULL,
	[Name] [varchar](255) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED COLUMNSTORE INDEX
)
GO

DROP TABLE zhuravel_schema.Vendor
GO

CREATE TABLE [zhuravel_schema].[Vendor]
WITH
(
  DISTRIBUTION = ROUND_ROBIN
)
AS SELECT DISTINCT VendorID FROM [zhuravel_schema].[fact_tripdata]
WHERE VendorID IS NOT NULL;
GO

ALTER TABLE [zhuravel_schema].[Vendor]
ADD [Name] varchar(50) NULL;
GO

CREATE TABLE [zhuravel_schema].[RateCode]
WITH
(
  DISTRIBUTION = ROUND_ROBIN
)
AS SELECT DISTINCT RateCodeID FROM [zhuravel_schema].[fact_tripdata]
WHERE RateCodeID IS NOT NULL AND RateCodeID != 99;
GO

ALTER TABLE [zhuravel_schema].[RateCode]
ADD [Name] varchar(50) NULL;
GO

CREATE TABLE [zhuravel_schema].[Payment_type]
WITH
(
  DISTRIBUTION = ROUND_ROBIN
)
AS SELECT DISTINCT payment_type FROM [zhuravel_schema].[fact_tripdata]
WHERE payment_type IS NOT NULL;

ALTER TABLE [zhuravel_schema].[Payment_type]
ADD [Name] varchar(50) NULL;