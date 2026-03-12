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
# MAGIC VALUES ('dv_customer', '1900-01-01 00:00:00')


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

watermark_df = spark.table("pipeline_watermark") \
    .where(col("table_name") == "dv_customer") \
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
          .where(col("modifiedon") > watermark)

display(df26)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Update a watermark table after pipeline execution**

# CELL ********************

from pyspark.sql.functions import max

max_timestamp = df.select(max("modifiedon")).collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
