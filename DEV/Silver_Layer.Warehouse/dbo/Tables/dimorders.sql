CREATE TABLE [dbo].[dimorders] (

	[order_id] int NOT NULL, 
	[order_date] varchar(8000) NULL, 
	[requested_ship_date] varchar(8000) NULL, 
	[promised_ship_date] varchar(8000) NULL, 
	[days_to_ship] varchar(8000) NULL, 
	[lead_time_days] varchar(8000) NULL, 
	[sla_days] varchar(8000) NULL, 
	[order_status] varchar(8000) NULL, 
	[priority] varchar(8000) NULL, 
	[order_category] varchar(8000) NULL, 
	[payment_status] varchar(8000) NULL, 
	[uom] varchar(8000) NULL, 
	[qty] varchar(8000) NULL, 
	[unit_price] varchar(8000) NULL, 
	[line_amount] varchar(8000) NULL, 
	[tax_rate] varchar(8000) NULL, 
	[tax_amount] varchar(8000) NULL, 
	[fulfillment_channel] varchar(8000) NULL, 
	[start_column] datetime2(6) NULL, 
	[end_date] datetime2(6) NULL, 
	[Iscurrent] int NULL, 
	[hash] varchar(8000) NULL
);


GO
ALTER TABLE [dbo].[dimorders] ADD CONSTRAINT Pk_dimorders primary key NONCLUSTERED ([order_id]);