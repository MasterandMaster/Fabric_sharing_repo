CREATE TABLE [dbo].[dimcustomer] (

	[customer_id] varchar(8000) NULL, 
	[customer_name] varchar(8000) NULL, 
	[customer_segment] varchar(8000) NULL, 
	[region] varchar(8000) NULL, 
	[country] varchar(8000) NULL, 
	[state] varchar(8000) NULL, 
	[city] varchar(8000) NULL, 
	[postal_code] varchar(8000) NULL, 
	[contact_email] varchar(8000) NULL, 
	[customer_key] int NOT NULL, 
	[start_column] datetime2(6) NULL, 
	[end_date] datetime2(6) NULL, 
	[Iscurrent] int NULL, 
	[hash] varchar(8000) NULL, 
	[Password] varchar(50) NULL
);