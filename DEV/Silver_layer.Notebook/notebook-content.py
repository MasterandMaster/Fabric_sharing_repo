# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9f05f3f2-8031-422e-92e6-6e6ba2566a14",
# META       "default_lakehouse_name": "Silver_layer_L",
# META       "default_lakehouse_workspace_id": "4d148c35-b745-498d-a028-d30a45dc644a",
# META       "known_lakehouses": [
# META         {
# META           "id": "076549b1-8219-4d46-9868-734759666ec3"
# META         },
# META         {
# META           "id": "9f05f3f2-8031-422e-92e6-6e6ba2566a14"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bring dim data from bronze layer and rename them as source table

# CELL ********************

df1 = spark.read.table('Bronze_Layer.dbo.dimcustomer')
df1.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('sr_customer')

df2 = spark.read.table('Bronze_Layer.dbo.dimproduct')
df2.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('sr_product')

df3 = spark.read.table('Bronze_Layer.dbo.factorder')
df3.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('sr_factorder')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **adding a source_column**

# CELL ********************

df21 = spark.read.table('sr_customer')
df22 = spark.read.table('sr_product')

df23 = df21.withColumn('Source_column', lit('source').cast('string'))
df24 = df22.withColumn('Source_column', lit('source').cast('string'))

df23.write.mode('overwrite').option('mergeSchema','true').saveAsTable('sr_customer')
df24.write.mode('overwrite').option('mergeSchema','true').saveAsTable('sr_product')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Create a structure for a dataverse table**

# CELL ********************

df4 = spark.read.table('Bronze_Layer.dbo.dimcustomer')
df4.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('dv_customer')

df5 = spark.read.table('Bronze_Layer.dbo.dimproduct')
df5.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('dv_product')

df6 = spark.read.table('Bronze_Layer.dbo.factorder')
df6.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('dv_factorder')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC TRUNCATE Table dv_customer;
# MAGIC TRUNCATE Table dv_product;
# MAGIC TRUNCATE Table dv_factorder


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Create a configuration table for dynamically ingest multiple table from dataverse through datapipeline**

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE dataverse_table_config (
# MAGIC dataverse_table STRING,
# MAGIC lakehouse_table STRING
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO dataverse_table_config VALUES
# MAGIC ('cr31c_customer','dv_customer'),
# MAGIC ('cr31c_product','dv_product'),
# MAGIC ('cr31c_f_order','dv_factorder')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create a  stagging table - Stagging table stores the data from store before performing a scd type 2. In these case two sources are there then need to combine the data using union**

# CELL ********************

from pyspark.sql.functions import *
df7 = spark.read.table('dv_customer')
df8 = df7.select(col('cr31c_city').alias('city'),col('cr31c_contactemail').alias('contact_email'),
col('cr31c_country').alias('country'),col('cr31c_customerid1').alias('customer_id'),
col('cr31c_customerkey'),col('cr31c_customername').alias('customer_name'),
col('cr31c_customersegment').alias('customer_segment'),col('cr31c_postalcode').alias('postal_code'),
col('cr31c_region').alias('region'),col('cr31c_state').alias('state'),col('modifiedon').alias('load_date'))
df8.write.mode('overwrite').option('mergeSchema','true').saveAsTable('stg_customer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df9 = spark.read.table('dv_product')
df10 = df9.select(col('cr31c_category').alias('category'),col('cr31c_color').alias('color'),
col('cr31c_currency').alias('currency'),col('cr31c_productname').alias('product_name'),
col('cr31c_productid1'),col('cr31c_subcategory').alias('sub_category'),
col('cr31c_taxrate').alias('tax_rate'),col('cr31c_unitprice').alias('unit_price'))
df10.write.mode('overwrite').option('mergeSchema','true').saveAsTable('stg_product')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df65 = spark.read.table('Bronze_Layer.dbo.factorder')
df66 = spark.read.table('dv_factorder')

df65.printSchema()
df66.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
df68 = spark.read.table('dv_factorder')
df69 = df68.select(

    col('cr31c_orderid1').alias('order_id'),
    col('cr31c_orderdate').alias('order_date'),
    col('cr31c_requestedshipdate').alias('requested_ship_date'),
    col('cr31c_promisedshipdate').alias('promised_ship_date'),
    col('cr31c_daystoship').alias('days_to_ship'),
    col('cr31c_leadtimedays').alias('lead_time_days'),
    col('cr31c_sladays').alias('sla_days'),
    col('cr31c_orderstatus').alias('order_status'),
    col('cr31c_priority').alias('priority'),
    col('cr31c_ordercategory').alias('order_category'),

    col('cr31c_customerid').alias('customer_id'),
    col('cr31c_customername').alias('customer_name'),
    col('cr31c_customersegment').alias('customer_segment'),

    col('cr31c_region').alias('region'),
    col('cr31c_country').alias('country'),
    col('cr31c_state').alias('state'),
    col('cr31c_city').alias('city'),
    col('cr31c_postalcode').alias('postal_code'),

    col('cr31c_contactemail').alias('contact_email'),
    col('cr31c_contactphone').alias('contact_phone'),

    col('cr31c_paymentterms').alias('payment_terms'),
    col('cr31c_paymentstatus').alias('payment_status'),

    col('cr31c_sku').alias('sku'),
    col('cr31c_productname').alias('product_name'),
    col('cr31c_skucolor').alias('sku_color'),

    col('cr31c_category').alias('category'),
    col('cr31c_subcategory').alias('sub_category'),

    col('cr31c_unitofmeasure').alias('uom'),
    col('cr31c_quantity').alias('qty'),
    col('cr31c_unitprice').alias('unit_price'),
    col('cr31c_currency').alias('currency'),

    col('cr31c_lineamount').alias('line_amount'),
    col('cr31c_taxrate').alias('tax_rate'),
    col('cr31c_taxamount').alias('tax_amount'),
    col('cr31c_totalamount').alias('total_amount'),

    col('cr31c_fulfillmentchannel').alias('fulfillment_channel'),

    col('cr31c_sourcesystem').alias('source_system'),
    col('cr31c_normalizedsourcesystem').alias('source_system_normalized'),

    col('cr31c_ingesttimestamp').alias('ingest_ts'),
    col('cr31c_isvalid').alias('is_valid'),
    col('cr31c_dataqualitynotes').alias('data_quality_notes'),

    col('cr31c_createdby').alias('created_by'),
    col('cr31c_lastupdatedtimestamp').alias('last_updated_ts'),

    col('cr31c_productid').alias('product_id')

)

df69.write.mode('overwrite').option('mergeSchema','true').saveAsTable('stg_factorder')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **add start_column, end_date, Iscurrent and  hash columns**

# CELL ********************

from pyspark.sql.functions import *

df11 = spark.read.table('stg_customer')
df12 = df11.withColumn("start_column", current_timestamp())\
    .withColumn("end_date", lit("9999-12-31").cast("timestamp"))\
    .withColumn("Iscurrent", lit(1).cast("int"))\
    .withColumn("hash",sha2(concat_ws("|","customer_id","customer_name","customer_segment",
    "region","country","state","city","postal_code","contact_email"),256))\
    .withColumn('Source_column', lit('dataverse').cast('string'))
    

#df13 = spark.read.table("stg_product")
#df14 = df13.withColumn("start_column", current_timestamp())\
 #   .withColumn("end_date", lit("9999-12-31").cast("timestamp"))\
  #  .withColumn("Iscurrent", lit(1).cast("int"))\
   # .withColumn("hash",sha2(concat_ws("|","product_name","category","sub_category",
    #"color", "unit_price","currency","tax_rate"),256))\
    #.withColumn('Source_column', lit('dataverse').cast('string'))
 
df12.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("stg_customer")   
#df14.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("stg_product")  



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **combine the source table with the stagging table**

# CELL ********************

df15 = spark.read.table('stg_customer')
#df16 = spark.read.table('stg_product')
df17 = spark.read.table('sr_customer')
#df18 = spark.read.table('sr_product')

df19 = df15.unionByName(df17)
#df20 = df16.unionByName(df18)

df19.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("stg_customer_master")   
#df20.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("stg_product_master")  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Create dim table which based on soruce table which will further merge with stagging table**

# CELL ********************

# MAGIC %%sql
# MAGIC Create Table dim_customer as SELECT * from sr_customer;
# MAGIC CREATE Table dim_product as Select * from sr_product

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Merge statement**

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC MERGE INTO dim_customer AS target
# MAGIC USING stg_customer_master AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC AND target.Iscurrent = 1
# MAGIC 
# MAGIC WHEN MATCHED
# MAGIC AND target.hash <> source.hash
# MAGIC THEN UPDATE SET
# MAGIC     target.end_date = current_timestamp(),
# MAGIC     target.Iscurrent = 0
# MAGIC     
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     customer_segment,
# MAGIC     region,
# MAGIC     country,
# MAGIC     state,
# MAGIC     city,
# MAGIC     postal_code,
# MAGIC     contact_email,
# MAGIC     cr31c_customerkey,
# MAGIC     start_column,
# MAGIC     end_date,
# MAGIC     Iscurrent,
# MAGIC     hash,
# MAGIC     Source_column
# MAGIC )
# MAGIC 
# MAGIC VALUES (
# MAGIC     source.customer_id,
# MAGIC     source.customer_name,
# MAGIC     source.customer_segment,
# MAGIC     source.region,
# MAGIC     source.country,
# MAGIC     source.state,
# MAGIC     source.city,
# MAGIC     source.postal_code,
# MAGIC     source.contact_email,
# MAGIC     source.cr31c_customerkey,
# MAGIC     current_timestamp(),
# MAGIC     NULL,
# MAGIC     1,
# MAGIC     source.hash,
# MAGIC     source.Source_column
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create a table to store a last modify date of pipeline**

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS pipeline_watermark (
# MAGIC table_name STRING,
# MAGIC last_watermark TIMESTAMP
# MAGIC )


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **This default date will help to load all data through pipeline in lakehouse**

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO pipeline_watermark
# MAGIC VALUES ('customer', '1900-01-01 00:00:00')


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

watermark_df = spark.table("pipeline_watermark") \
    .where(col("table_name") == "customer") \
    .select("last_watermark")

watermark = watermark_df.collect()[0][0]

display(watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df26 = spark.table("dv_customer") \
          .select(col('cr31c_city').alias('city'),col('cr31c_contactemail').alias('contact_email'),
           col('cr31c_country').alias('country'),col('cr31c_customerid1').alias('customer_id'),
           col('cr31c_customerkey'),col('cr31c_customername').alias('customer_name'),
           col('cr31c_customersegment').alias('customer_segment'),col('cr31c_postalcode').alias('postal_code'),
           col('cr31c_region').alias('region'),col('cr31c_state').alias('state'),col('modifiedon').alias('load_date'))\
          .where(col("modifiedon") > watermark)


display(df26)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Select * from stg_customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df27.write.mode("append").saveAsTable("stg_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

df27 = df26.withColumn("start_column", current_timestamp())\
    .withColumn("end_date", lit("9999-12-31").cast("timestamp"))\
    .withColumn("hash",sha2(concat_ws("|","customer_id","customer_name","customer_segment",
    "region","country","state","city","postal_code","contact_email"),256))\
    .withColumn('Source_column', lit('dataverse').cast('string'))

display(df27)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df27 = spark.table("dv_customer") \
          .where(col("modifiedon") > watermark)

display(df27)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Update a watermark table after pipeline execution**

# CELL ********************

from pyspark.sql.functions import max

max_timestamp = df26.select(max("load_date")).collect()[0][0]

display(max_timestamp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

delta_table = DeltaTable.forName(spark, "pipeline_watermark")

delta_table.update(
    condition = col("table_name") == "customer",
    set = {"last_watermark": lit(max_timestamp)}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **left join to detect the changes**

# CELL ********************

stg_customer = spark.read.table('stg_customer')
dim_customer = spark.read.table('dim_customer')

changes = stg_customer.alias("s").join(
    dim_customer.alias("d"),
    "customer_id",
    "left"
).withColumn(
    "change_type",
    when(dim_customer.customer_id.isNull(), "INSERT")
    .when(stg_customer .hash != dim_customer.hash, "UPDATE")
    .otherwise("NO_CHANGE")
)

display(changes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --Select * from dim_customer
# MAGIC 
# MAGIC --update dim_customer Set Iscurrent = 1 where customer_id = 'C002'
# MAGIC delete from dim_customer where Iscurrent = 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Start

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO dim_customer AS target
# MAGIC USING stg_customer_master AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC AND target.Iscurrent = 1
# MAGIC 
# MAGIC WHEN MATCHED
# MAGIC AND target.hash <> source.hash
# MAGIC THEN UPDATE SET
# MAGIC target.end_date = current_timestamp(),
# MAGIC target.Iscurrent = 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO dim_customer (
# MAGIC customer_id,
# MAGIC customer_name,
# MAGIC customer_segment,
# MAGIC region,
# MAGIC country,
# MAGIC state,
# MAGIC city,
# MAGIC postal_code,
# MAGIC contact_email,
# MAGIC cr31c_customerkey,
# MAGIC start_column,
# MAGIC end_date,
# MAGIC Iscurrent,
# MAGIC hash,
# MAGIC Source_column,
# MAGIC load_timestamp,
# MAGIC load_date
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC s.customer_id,
# MAGIC s.customer_name,
# MAGIC s.customer_segment,
# MAGIC s.region,
# MAGIC s.country,
# MAGIC s.state,
# MAGIC s.city,
# MAGIC s.postal_code,
# MAGIC s.contact_email,
# MAGIC s.cr31c_customerkey,
# MAGIC current_timestamp(),
# MAGIC timestamp('9999-12-31'),
# MAGIC 1,
# MAGIC s.hash,
# MAGIC s.Source_column,
# MAGIC s.load_timestamp,
# MAGIC s.load_date
# MAGIC 
# MAGIC FROM stg_customer_master s
# MAGIC JOIN dim_customer d
# MAGIC ON s.customer_id = d.customer_id
# MAGIC 
# MAGIC WHERE d.Iscurrent = 0
# MAGIC AND s.hash <> d.hash

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO dim_customer (
# MAGIC customer_id,
# MAGIC customer_name,
# MAGIC customer_segment,
# MAGIC region,
# MAGIC country,
# MAGIC state,
# MAGIC city,
# MAGIC postal_code,
# MAGIC contact_email,
# MAGIC cr31c_customerkey,
# MAGIC start_column,
# MAGIC end_date,
# MAGIC Iscurrent,
# MAGIC hash,
# MAGIC Source_column,
# MAGIC load_timestamp,
# MAGIC load_date
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC s.customer_id,
# MAGIC s.customer_name,
# MAGIC s.customer_segment,
# MAGIC s.region,
# MAGIC s.country,
# MAGIC s.state,
# MAGIC s.city,
# MAGIC s.postal_code,
# MAGIC s.contact_email,
# MAGIC s.cr31c_customerkey,
# MAGIC current_timestamp(),
# MAGIC TIMESTAMP('9999-12-31'),
# MAGIC 1,
# MAGIC s.hash,
# MAGIC s.Source_column,
# MAGIC s.load_timestamp,
# MAGIC s.load_date
# MAGIC 
# MAGIC FROM stg_customer_master s
# MAGIC LEFT JOIN dim_customer d
# MAGIC ON s.customer_id = d.customer_id
# MAGIC 
# MAGIC WHERE d.customer_id IS NULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df29 = spark.read.table('dim_customer')
df30 = spark.read.table('stg_customer_master')

df29.printSchema()
df30.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df31 = spark.read.table('dim_customer')
df32 = df31.drop_duplicates(['customer_id'])
df32.write.mode('overwrite').saveAsTable('dim_customer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC update dim_customer Set city = 'Bengluru'where customer_id = 'C002'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC target.customer_id,
# MAGIC target.city,
# MAGIC target.customer_name AS old_customer_name,
# MAGIC source.customer_name AS new_customer_name,
# MAGIC target.hash AS old_hash,
# MAGIC source.hash AS new_hash,
# MAGIC target.Iscurrent,
# MAGIC target.start_column,
# MAGIC target.end_date
# MAGIC FROM dim_customer target
# MAGIC JOIN stg_customer_master source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHERE target.Iscurrent = 1
# MAGIC AND target.hash <> source.hash

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC TRUNCATE table stg_customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC update dim_customer set Iscurrent = 1 where city = 'Mangaluru'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions  import *
df51 = spark.read.table('dim_customer')
df52 = df51.dropDuplicates(['city'])
display(df52)
#df52.write.mode('overwrite').saveAsTable('dim_customer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY stg_product

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM stg_product VERSION AS OF 8

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC RESTORE TABLE stg_product TO VERSION AS OF 8

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Truncate Table pipeline_watermark

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE dim_customer ADD COLUMNS (
# MAGIC load_timestamp TIMESTAMP,
# MAGIC load_date TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df53 = spark.read.table('dim_customer')
df53.withColumn('load_timestamp', col('load_timestamp').cast('timestamp'))\
.withColumn('load_date', col('load_date').cast('timestamp'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from dim_customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Alter Table stg_product Add Column Batch_id INT

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from stg_product

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import max

max_batch_id = spark.table("stg_product") \
                    .select(max("batch_id")) \
                    .collect()[0][0]

display(max_batch_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_batch = max_batch_id.collect()[0]["max_batch_id"]

if current_batch is None:
    current_batch = 1
else:
    current_batch = current_batch + 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import max, coalesce, lit

max_batch = spark.table("stg_product") \
                 .select(coalesce(max("batch_id"), lit(0)).alias("max_batch")) \
                 .first()["max_batch"]

current_batch = max_batch + 1

display(current_batch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df55 = spark.read.table('stg_product')
df56 = df55.withColumn('batch_id',lit(current_batch))
display(df56)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df56.write.mode('append').saveAsTable('stg_customer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from dim_customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df = spark.table("dim_customer")

target = df.filter(col("customer_id") == "C003").dropDuplicates(["customer_id","hash"])
others = df.filter(col("customer_id") != "C003")

result = target.unionByName(others)
result.write.mode('overwrite').saveAsTable('dim_customer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df60 = spark.read.table('dim_product')
df61 = spark.read.table('stg_product')

df60.printSchema()
df61.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from dim_product

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from stg_product

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE Table factOrder as select * from sr_factorder

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Alter Table stg_factorder Add column Batch_id INT

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Select * from stg_factorder

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Delete from stg_factorder where Batch_id IS NULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import max

max_key = spark.table("dim_customer") \
               .selectExpr("max(surrogate_key) as max_key") \
               .first()["max_key"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
