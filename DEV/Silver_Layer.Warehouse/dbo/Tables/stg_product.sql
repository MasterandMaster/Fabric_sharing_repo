CREATE TABLE [dbo].[stg_product] (

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
ALTER TABLE [dbo].[stg_product] ADD CONSTRAINT Pk_product primary key NONCLUSTERED ([product_id]);