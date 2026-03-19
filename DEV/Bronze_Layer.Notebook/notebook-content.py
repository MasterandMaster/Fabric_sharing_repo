# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "076549b1-8219-4d46-9868-734759666ec3",
# META       "default_lakehouse_name": "Bronze_Layer",
# META       "default_lakehouse_workspace_id": "4d148c35-b745-498d-a028-d30a45dc644a",
# META       "known_lakehouses": [
# META         {
# META           "id": "076549b1-8219-4d46-9868-734759666ec3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # <u>**Read the record from the csv file**</u>

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_Layer.dbo.dimcustomer LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("header",True).format("csv").load("abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Bronze_Layer.Lakehouse/Files/Orders_data.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # <u>**Use Case statement to replace the sub category values**</u>

# CELL ********************

from pyspark.sql import functions as F

from pyspark.sql import functions as F

df1 = (
    df.withColumn(
        "sub_category",
        F.when(F.col("product_name") == "LG TVS", F.lit("Television"))
         .when(F.col("product_name") == "Samsung Mobile", F.lit("Smart Phones"))
         .when(F.col("product_name") == "Boat Watches", F.lit("Smart Watches"))
         .when(F.col("product_name") == "Dell Laptops", F.lit("Dell Laptops"))
         .when(F.col("product_name") == "Batteries", F.lit("Battery"))
         .otherwise(F.lit("Audio"))
    )
)

display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # <u>**Dimension Create Dimcustomer**</u>

# CELL ********************

df2 = df1.select("customer_id","customer_name","customer_segment","region","country","state",
"city","postal_code","contact_email")
df3 = df2.drop_duplicates(["customer_id"])
df3.write.format("delta").mode("overwrite").saveAsTable("DimCustomer")
display(df3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # <u>**Dimension Create Dimproduct**</u>

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Select the columns you need
df4 = df1.select("product_name", "category", "sub_category",F.col("sku_color").alias("color"),
"unit_price","currency","tax_rate")

# Keep one row per product_name (simple dimension)
df5 = df4.dropDuplicates(["product_name"])

# Define a window spec (global ordering by product_name)
w = Window.orderBy("product_name")

# Add a row_number() as a surrogate integer id
df6 = df5.withColumn("product_id", F.row_number().over(w))

df6.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("dimproduct")

display(df6)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inner Join between dimproduct and factorder only to have a product id column from dimproduct table**

# CELL ********************

from pyspark.sql import functions as F
df1_a = df1.alias("a")
df6_b = df6.alias("b")
df7 = df6_b.join(df1_a, F.col("b.product_name")==F.col("a.product_name"), 'inner')\
.select(F.col("a.*"),F.col("b.product_id"))
df7.write.mode("overwrite").format("delta").saveAsTable("factorder")
display(df7)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **To generate a incremental surrogate key in dimcustomer dimensional tables using row number**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df8 = spark.read.table("dimcustomer")
w = Window.orderBy("customer_id")
df9 = df8.withColumn("customer_key", F.row_number().over(w))
df9.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("dimcustomer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Add a start, end end date,Iscurrent and Hash column in all dimension tables **

# CELL ********************

from pyspark.sql.functions import *

df10 = df9.withColumn("start_column", current_timestamp())\
    .withColumn("end_date", lit("9999-12-31").cast("timestamp"))\
    .withColumn("Iscurrent", lit(1).cast("int"))\
    .withColumn("hash",sha2(concat_ws("|","customer_id","customer_name","customer_segment",
    "region","country","state","city","postal_code","contact_email"),256))

df11 = spark.read.table("dimproduct")
df12 = df11.withColumn("start_column", current_timestamp())\
    .withColumn("end_date", lit("9999-12-31").cast("timestamp"))\
    .withColumn("Iscurrent", lit(1).cast("int"))\
    .withColumn("hash",sha2(concat_ws("|","product_name","category","sub_category",
    "color", "unit_price","currency","tax_rate"),256))
 
df10.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dimcustomer")   
df12.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dimproduct")  



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Create a New Order table from fact table**

# CELL ********************



df13 = spark.read.table("factorder")
df14 = df13.select("order_id", "order_date","requested_ship_date", "promised_ship_date","days_to_ship","lead_time_days","sla_days",
"order_status", "priority", "order_category", "payment_status", "uom", "qty", "unit_price", "line_amount", "tax_rate", "tax_amount",
"fulfillment_channel")
df15 = df14.withColumn("order_id",col("order_id").cast("int"))
df16 = df15.drop_duplicates(["order_id"])
df17 = df16.withColumn("start_column", current_timestamp())\
    .withColumn("end_date", lit("9999-12-31").cast("timestamp"))\
    .withColumn("Iscurrent", lit(1).cast("int"))\
    .withColumn("hash",sha2(concat_ws("|","order_id"),256))
df17.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dimorders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Create a Stagging table **

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE Table stg_customer as SELECT * from dimcustomer;
# MAGIC 
# MAGIC CREATE Table stg_orders as SELECT * from dimorders;
# MAGIC 
# MAGIC CREATE Table stg_product as SELECT * from dimproduct

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE stg_customer ADD CONSTRAINT PK_customerkey 
# MAGIC PRIMARY KEY (customer_key) NOT Enforced

# METADATA ********************

# META {
# META   "language": "sparksql",
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

# MARKDOWN ********************

# **Change the customer_key data type**

# CELL ********************

from pyspark.sql.functions import *

df18 = spark.read.table('dimcustomer')
df19 = df18.withColumn('customer_key',col('customer_key').cast('string'))
df19.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('dimcustomer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **change the product_id datatype**

# CELL ********************

from pyspark.sql.functions import *

df20 = spark.read.table('dimproduct')
df21 = df20.withColumn('product_id',col('product_id').cast('string'))
df21.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('dimproduct')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from factorder

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df22 = spark.read.table('factorder')
df23 = df22.withColumn('product_id',col('product_id').cast('string'))
df23.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('factorder')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Create a mapping table for pipeline**

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE table_mapping (
# MAGIC     lakehouse_table STRING,
# MAGIC     dataverse_table STRING,
# MAGIC     source_key STRING,
# MAGIC     destination_key STRING,
# MAGIC     is_active INT
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO table_mapping VALUES
# MAGIC ('dimcustomer','cr31c_customer','cr31c_customerkey','cr31c_customer_key',1),
# MAGIC ('dimproduct','cr31c_product','cr31c_productid1','cr31c_product_key',1)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Select * from table_mapping

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df24 = spark.read.table('dimcustomer')
df25 = df24.withColumnRenamed('customer_key','cr31c_customerkey')
df25.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('dimcustomer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE mapping_info (
# MAGIC     lakehouse_table STRING,
# MAGIC     dataverse_table STRING,
# MAGIC     source_column STRING,
# MAGIC     destination_column STRING,
# MAGIC     destination_key string,
# MAGIC     is_key INT,
# MAGIC     is_active INT
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO mapping_info VALUES
# MAGIC --('dimcustomer','cr31c_customer','cr31c_customerkey','cr31c_customerkey','cr31c_customer_key',1,1),
# MAGIC --('dimcustomer','cr31c_customer','customer_name','cr31c_customername','cr31c_customer_key',0,1)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from mapping_info where lakehouse_table = 'dimproduct'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO mapping_info VALUES
# MAGIC ('dimcustomer','cr31c_customer','customer_id','cr31c_customerid1','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','customer_segment','cr31c_customersegment','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','region','cr31c_region','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','country','cr31c_country','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','state','cr31c_state','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','city','cr31c_city','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','postal_code','cr31c_postalcode','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','contact_email','cr31c_contactemail','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','start_column','cr31c_startdate','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','end_date','cr31c_enddate','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','Iscurrent','cr31c_is_current','cr31c_customer_key',0,1),
# MAGIC ('dimcustomer','cr31c_customer','hash','cr31c_recordhash','cr31c_customer_key',0,1);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df26 = spark.read.table('mapping_info')
df27 = df26.drop_duplicates(['source_column'])
df27.write.mode('overwrite').saveAsTable('dimcustomer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO mapping_info VALUES
# MAGIC ('dimproduct','cr31c_product','cr31c_productid1','cr31c_productid1','cr31c_product_key',1,1),
# MAGIC ('dimproduct','cr31c_product','product_name','cr31c_productname','cr31c_product_key',0,1),
# MAGIC ('dimproduct','cr31c_product','category','cr31c_category','cr31c_product_key',0,1),
# MAGIC ('dimproduct','cr31c_product','sub_category','cr31c_subcategory','cr31c_product_key',0,1),
# MAGIC ('dimproduct','cr31c_product','color','cr31c_color','cr31c_product_key',0,1),
# MAGIC ('dimproduct','cr31c_product','unit_price','cr31c_unitprice','cr31c_product_key',0,1),
# MAGIC ('dimproduct','cr31c_product','currency','cr31c_currency','cr31c_product_key',0,1),
# MAGIC ('dimproduct','cr31c_product','tax_rate','cr31c_taxrate','cr31c_product_key',0,1),
# MAGIC ('dimproduct','cr31c_product','start_column','cr31c_startdate','cr31c_product_key',0,1),
# MAGIC ('dimproduct','cr31c_product','end_date','cr31c_enddate','cr31c_product_key',0,1),
# MAGIC ('dimproduct','cr31c_product','Iscurrent','cr31c_iscurrent','cr31c_product_key',0,1),
# MAGIC ('dimproduct','cr31c_product','hash','cr31c_hash','cr31c_product_key',0,1);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("factorder")

column_count = len(df.columns)

print(column_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO mapping_info VALUES
# MAGIC ('factorder',' cr31c_f_order','order_id','cr31c_orderid','cr31c_order_key',1,1),
# MAGIC ('factorder','cr31c_f_order','order_date','cr31c_orderdate','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','requested_ship_date','cr31c_requestedshipdate','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','promised_ship_date','cr31c_promisedshipdate','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','days_to_ship','cr31c_daystoship','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','lead_time_days','cr31c_leadtimedays','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','sla_days','cr31c_sladays','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','order_status','cr31c_orderstatus','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','priority','cr31c_priority','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','order_category','cr31c_ordercategory','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Customer Details
# MAGIC ('factorder','cr31c_f_order','customer_id','cr31c_customerid','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','customer_name','cr31c_customername','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','customer_segment','cr31c_customersegment','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Geography
# MAGIC ('factorder','cr31c_f_order','region','cr31c_region','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','country','cr31c_country','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','state','cr31c_state','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','city','cr31c_city','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','postal_code','cr31c_postalcode','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Contact
# MAGIC ('factorder','cr31c_f_order','contact_email','cr31c_contactemail','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','contact_phone','cr31c_contactphone','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Payment
# MAGIC ('factorder','cr31c_f_order','payment_terms','cr31c_paymentterms','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','payment_status','cr31c_paymentstatus','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Product Details
# MAGIC ('factorder','cr31c_f_order','sku','cr31c_sku','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','product_name','cr31c_productname','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','sku_color','cr31c_skucolor','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','category','cr31c_category','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','sub_category','cr31c_subcategory','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Quantity & Pricing
# MAGIC ('factorder','cr31c_f_order','uom','cr31c_uom','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','qty','cr31c_quantity','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','unit_price','cr31c_unitprice','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','currency','cr31c_currency','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Amounts
# MAGIC ('factorder','cr31c_f_order','line_amount','cr31c_lineamount','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','tax_rate','cr31c_taxrate','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','tax_amount','cr31c_taxamount','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','total_amount','cr31c_totalamount','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Logistics
# MAGIC ('factorder','cr31c_f_order','fulfillment_channel','cr31c_fulfillmentchannel','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Source Tracking
# MAGIC ('factorder','cr31c_f_order','source_system','cr31c_sourcesystem','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','source_system_normalized','cr31c_normalizedsourcesystem','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Audit Columns
# MAGIC ('factorder','cr31c_f_order','ingest_ts','cr31c_ingesttimestamp','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','is_valid','cr31c_isvalid','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','data_quality_notes','cr31c_dataqualitynotes','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','created_by','cr31c_createdby','cr31c_order_key',0,1),
# MAGIC ('factorder','cr31c_f_order','last_updated_ts','cr31c_lastupdatedtimestamp','cr31c_order_key',0,1),
# MAGIC 
# MAGIC -- Product ID
# MAGIC ('factorder','cr31c_f_order','product_id','cr31c_productid','cr31c_order_key',0,1);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

df30 = spark.read.table('factorder')

window_spec = Window.partitionBy("order_id") \
                    .orderBy(col("order_id").desc())

df_with_rn = df30.withColumn(
    "rn",
    row_number().over(window_spec)
)

display(df_with_rn)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df27 = spark.read.table('factorder')
df27.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from mapping_info where lakehouse_table = 'factorder'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
