CREATE TABLE [dbo].[dimproduct] (

	[product_name] varchar(8000) NULL, 
	[category] varchar(8000) NULL, 
	[sub_category] varchar(8000) NULL, 
	[product_id] int NOT NULL, 
	[color] varchar(8000) NULL, 
	[unit_price] varchar(8000) NULL, 
	[currency] varchar(8000) NULL, 
	[tax_rate] varchar(8000) NULL, 
	[start_column] datetime2(6) NULL, 
	[end_date] datetime2(6) NULL, 
	[Iscurrent] int NULL, 
	[hash] varchar(8000) NULL
);


GO
ALTER TABLE [dbo].[dimproduct] ADD CONSTRAINT Pk_dimproduct primary key NONCLUSTERED ([product_id]);