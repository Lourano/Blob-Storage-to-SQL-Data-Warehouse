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

CREATE TABLE [zhuravel_schema].Vendor
(
  id int NULL,
  name varchar(255) NULL
)
WITH
(
  DISTRIBUTION = REPLICATE,
  CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [zhuravel_schema].RateCode
(
  ID int NOT NULL,
  Name varchar(50) NULL
)
WITH
(
  DISTRIBUTION = REPLICATE,
  CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [zhuravel_schema].Payment_type
(
  ID int NOT NULL,
  Name varchar(50) NULL
)
WITH
(
  DISTRIBUTION = REPLICATE,
  CLUSTERED COLUMNSTORE INDEX
)
GO