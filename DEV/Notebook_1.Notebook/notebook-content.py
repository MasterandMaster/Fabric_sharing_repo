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
# META           "id": "9f05f3f2-8031-422e-92e6-6e6ba2566a14"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **1. Create a Watermark Metadata Table**

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

# **2. Insert Initial Watermark (First Time Setup)**

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO pipeline_watermark
# MAGIC --VALUES ('customer', '1900-01-01 00:00:00')
# MAGIC VALUES ('factorder', '1900-01-01 00:00:00')


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **3. Step 3 - Read the Watermark Value in Pipeline**

# CELL ********************

from pyspark.sql.functions import col

watermark_df = spark.table("pipeline_watermark") \
    .where(col("table_name") == "customer") \
    .select("last_watermark")

watermark = watermark_df.collect()[0][0]

display(watermark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **For multiple tables**

# CELL ********************

from pyspark.sql.functions import col

tables = ["customer", "product", "factorder"]

watermark_dict = {}

for table in tables:
    watermark_df = spark.table("pipeline_watermark") \
        .where(col("table_name") == table) \
        .select("last_watermark")

    watermark = watermark_df.first()["last_watermark"]

    watermark_dict[table] = watermark

print(watermark_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **4. Step 4 -Use Watermark in Source Query**

# CELL ********************

from pyspark.sql.functions import col

df1 = spark.table("dv_customer") \
          .where(col("modifiedon") > watermark)

display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **For multiple table**

# CELL ********************

from pyspark.sql.functions import col

tables = ["customer", "product", "factorder"]

for table in tables:

    watermark = watermark_dict[table]

    source_table = f"dv_{table}"

    df = spark.table(source_table) \
              .where(col("modifiedon") > watermark)

    print(f"Incremental records for {table}")
    display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **5. Step 1 - After Pipeline Execution Update Watermark**

# CELL ********************

from pyspark.sql.functions import max

df4 = spark.read.table('stg_customer')

max_timestamp_customer = df4.select(max("load_date")).collect()[0][0]

display(max_timestamp_customer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import max

df7 = spark.read.table('stg_product')

max_timestamp_product = df7.select(max("load_date")).collect()[0][0]

display(max_timestamp_product)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import max

df12 = spark.read.table('stg_factorder')

max_timestamp_product = df12.select(max("load_date")).collect()[0][0]

display(max_timestamp_product)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **6.step 2 - Update Watermark Table**

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

delta_table = DeltaTable.forName(spark, "pipeline_watermark")

delta_table.update(
    condition = col("table_name") == "customer",
    set = {"last_watermark": lit(max_timestamp_customer)}
)

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
    condition = col("table_name") == "product",
    set = {"last_watermark": lit(max_timestamp_product)}
)

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
    condition = col("table_name") == "factorder",
    set = {"last_watermark": lit(max_timestamp_product)}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ** Step 5 - Batch patition**

# CELL ********************

from pyspark.sql.functions import max, coalesce, lit

max_batch = spark.table("stg_customer") \
                 .select(coalesce(max("batch_id"), lit(0)).alias("max_batch")) \
                 .first()["max_batch"]

current_batch = max_batch + 1

display(current_batch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **For multiple tables**

# CELL ********************

from pyspark.sql.functions import max, coalesce, lit

tables = ["customer", "product", "factorder"]

batch_dict = {}

for table in tables:

    max_batch = spark.table(f"stg_{table}") \
                     .select(coalesce(max("batch_id"), lit(0)).alias("max_batch")) \
                     .first()["max_batch"]

    current_batch = max_batch + 1

    batch_dict[table] = current_batch

print(batch_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **skip this step**

# CELL ********************

from pyspark.sql.functions import *
df55 = spark.read.table('stg_customer')
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

# MARKDOWN ********************

# **7. Step 6 - Load the data from source into the stagging table by comparing it with watermark **

# CELL ********************

from pyspark.sql.functions import col

customer_watermark = watermark_dict["customer"]

df2 = spark.table("dv_customer") \
          .select(col('cr31c_city').alias('city'),col('cr31c_contactemail').alias('contact_email'),
           col('cr31c_country').alias('country'),col('cr31c_customerid1').alias('customer_id'),
           col('cr31c_customerkey'),col('cr31c_customername').alias('customer_name'),
           col('cr31c_customersegment').alias('customer_segment'),col('cr31c_postalcode').alias('postal_code'),
           col('cr31c_region').alias('region'),col('cr31c_state').alias('state'),col('modifiedon').alias('load_date'))\
          .where(col("modifiedon") > customer_watermark)


display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

product_watermark = watermark_dict["product"]

df5 = spark.table("dv_product") \
    .select(col('cr31c_category').alias('category'),col('cr31c_color').alias('color'),
    col('cr31c_currency').alias('currency'),col('cr31c_productname').alias('product_name'),
    col('cr31c_productid1'),col('cr31c_subcategory').alias('sub_category'),
    col('cr31c_taxrate').alias('tax_rate'),col('cr31c_unitprice').alias('unit_price'),
    col('modifiedon').alias('load_date'))\
    .where(col('modifiedon') > product_watermark)

display(df5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

factorder_watermark = watermark_dict["factorder"]

df10 = spark.read.table('dv_factorder')\
    .select(

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

    col('cr31c_productid').alias('product_id'), col('modifiedon').alias('load_date'))\
    .where(col('modifiedon') > factorder_watermark)

display(df10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Step 7**

# CELL ********************

from pyspark.sql.functions import *

customer_current_batch = batch_dict["customer"]

df3 = df2.withColumn("start_column", current_timestamp())\
    .withColumn("end_date", lit("9999-12-31").cast("timestamp"))\
    .withColumn("hash",sha2(concat_ws("|","customer_id","customer_name","customer_segment",
    "region","country","state","city","postal_code","contact_email"),256))\
    .withColumn('batch_id',lit(customer_current_batch))\
    .withColumn('Source_column', lit('dataverse').cast('string'))

display(df3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

product_current_batch = batch_dict["product"]

df6 = df5.withColumn("start_column", current_timestamp())\
    .withColumn("end_date", lit("9999-12-31").cast("timestamp"))\
    .withColumn("hash",sha2(concat_ws("|","cr31c_productid1","category","color",
    "currency","product_name","sub_category","tax_rate","unit_price"),256))\
    .withColumn('batch_id',lit(product_current_batch))\
    .withColumn('Source_column', lit('dataverse').cast('string'))

display(df6)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

product_current_batch = batch_dict["factorder"]

df11 = df10.withColumn("start_column", current_timestamp())\
    .withColumn("end_date", lit("9999-12-31").cast("timestamp"))\
    .withColumn("hash",sha2(concat_ws("|","order_id","order_date","qty",
    "unit_price","tax_rate","fulfillment_channel"),256))\
    .withColumn('batch_id',lit(product_current_batch))\
    .withColumn('Source_column', lit('dataverse').cast('string'))

display(df11)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3.write.mode("append").saveAsTable("stg_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df6.write.mode("append").option("mergeSchema", "true").saveAsTable("stg_product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df11.write.mode("append").option("mergeSchema", "true").saveAsTable("stg_factorder")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Run query to detect the changes**

# MARKDOWN ********************

# **New batch wise changes**

# CELL ********************

from pyspark.sql.functions import col, when

latest_batch = spark.sql("""
SELECT MAX(batch_id) AS batch_id
FROM stg_customer
""").collect()[0]["batch_id"]

stg_customer = spark.read.table("stg_customer") \
    .filter(col("batch_id") == latest_batch)

dim_customer = spark.read.table("dim_customer") \
    .filter(col("Iscurrent") == 1)

changes = stg_customer.alias("s").join(
    dim_customer.alias("d"),
    col("s.customer_id") == col("d.customer_id"),
    "left"
).withColumn(
    "change_type",
    when(col("d.customer_id").isNull(), "INSERT")
    .when(col("s.hash") != col("d.hash"), "UPDATE")
    .otherwise("NO_CHANGE")
)

display(changes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when

latest_batch = spark.sql("""
SELECT MAX(batch_id) AS batch_id
FROM stg_product
""").collect()[0]["batch_id"]

stg_product = spark.read.table("stg_product") \
    .filter(col("batch_id") == latest_batch)

dim_product = spark.read.table("dim_product") \
    .filter(col("Iscurrent") == 1)

changes = stg_product.alias("s").join(
    dim_product.alias("d"),
    col("s.product_name") == col("d.product_name"),
    "left"
).withColumn(
    "change_type",
    when(col("d.product_name").isNull(), "INSERT")
    .when(col("s.hash") != col("d.hash"), "UPDATE")
    .otherwise("NO_CHANGE")
)

display(changes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Merge statement with batch partition**

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO dim_customer AS target
# MAGIC USING (
# MAGIC     SELECT *
# MAGIC     FROM stg_customer
# MAGIC     WHERE batch_id = (SELECT MAX(batch_id) FROM stg_customer)
# MAGIC ) AS source
# MAGIC 
# MAGIC ON target.customer_id = source.customer_id
# MAGIC AND target.Iscurrent = 1
# MAGIC 
# MAGIC WHEN MATCHED
# MAGIC AND target.hash <> source.hash
# MAGIC 
# MAGIC THEN UPDATE SET
# MAGIC target.end_date = current_timestamp(),
# MAGIC target.Iscurrent = 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Insert Updated records**

# CELL ********************

# MAGIC %%sql
# MAGIC 
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
# MAGIC FROM (
# MAGIC     SELECT *
# MAGIC     FROM stg_customer
# MAGIC     WHERE batch_id = (SELECT MAX(batch_id) FROM stg_customer)
# MAGIC ) s
# MAGIC 
# MAGIC WHERE NOT EXISTS (
# MAGIC 
# MAGIC SELECT 1
# MAGIC FROM dim_customer d
# MAGIC 
# MAGIC WHERE d.customer_id = s.customer_id
# MAGIC AND d.hash = s.hash
# MAGIC AND d.Iscurrent = 1
# MAGIC 
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Insert new records**

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
# MAGIC FROM (
# MAGIC     SELECT *
# MAGIC     FROM stg_customer
# MAGIC     WHERE batch_id = (SELECT MAX(batch_id) FROM stg_customer)
# MAGIC ) s
# MAGIC 
# MAGIC LEFT JOIN dim_customer d
# MAGIC ON s.customer_id = d.customer_id
# MAGIC 
# MAGIC WHERE d.customer_id IS NULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Merge for product**

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO dim_product AS target
# MAGIC USING (
# MAGIC     SELECT *
# MAGIC     FROM stg_product
# MAGIC     WHERE batch_id = (SELECT MAX(batch_id) FROM stg_product)
# MAGIC ) AS source
# MAGIC 
# MAGIC ON target.product_name = source.product_name
# MAGIC AND target.Iscurrent = 1
# MAGIC 
# MAGIC WHEN MATCHED
# MAGIC AND target.hash <> source.hash
# MAGIC 
# MAGIC THEN UPDATE SET
# MAGIC target.end_date = current_timestamp(),
# MAGIC target.Iscurrent = 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Insert updated row**

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO dim_product (
# MAGIC product_name,
# MAGIC category,
# MAGIC sub_category,
# MAGIC cr31c_productid1,
# MAGIC color,
# MAGIC unit_price,
# MAGIC currency,
# MAGIC tax_rate,
# MAGIC start_column,
# MAGIC end_date,
# MAGIC Iscurrent,
# MAGIC hash,
# MAGIC Source_column
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC s.product_name,
# MAGIC s.category,
# MAGIC s.sub_category,
# MAGIC s.cr31c_productid1,
# MAGIC s.color,
# MAGIC s.unit_price,
# MAGIC s.currency,
# MAGIC s.tax_rate,
# MAGIC current_timestamp(),
# MAGIC timestamp('9999-12-31'),
# MAGIC 1,
# MAGIC s.hash,
# MAGIC s.Source_column
# MAGIC 
# MAGIC FROM (
# MAGIC     SELECT *
# MAGIC     FROM stg_product
# MAGIC     WHERE Batch_id = (SELECT MAX(Batch_id) FROM stg_product)
# MAGIC ) s
# MAGIC 
# MAGIC WHERE NOT EXISTS (
# MAGIC 
# MAGIC SELECT 1
# MAGIC FROM dim_product d
# MAGIC 
# MAGIC WHERE d.product_name = s.product_name
# MAGIC AND d.hash = s.hash
# MAGIC AND d.Iscurrent = 1
# MAGIC 
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Insert new record**

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO dim_product (
# MAGIC product_name,
# MAGIC category,
# MAGIC sub_category,
# MAGIC cr31c_productid1,
# MAGIC color,
# MAGIC unit_price,
# MAGIC currency,
# MAGIC tax_rate,
# MAGIC start_column,
# MAGIC end_date,
# MAGIC Iscurrent,
# MAGIC hash,
# MAGIC Source_column
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC s.product_name,
# MAGIC s.category,
# MAGIC s.sub_category,
# MAGIC s.cr31c_productid1,
# MAGIC s.color,
# MAGIC s.unit_price,
# MAGIC s.currency,
# MAGIC s.tax_rate,
# MAGIC current_timestamp(),
# MAGIC TIMESTAMP('9999-12-31'),
# MAGIC 1,
# MAGIC s.hash,
# MAGIC s.Source_column
# MAGIC 
# MAGIC FROM (
# MAGIC     SELECT *
# MAGIC     FROM stg_product
# MAGIC     WHERE Batch_id = (SELECT MAX(Batch_id) FROM stg_product)
# MAGIC ) s
# MAGIC 
# MAGIC LEFT JOIN dim_product d
# MAGIC ON s.cr31c_productid1 = d.cr31c_productid1
# MAGIC 
# MAGIC WHERE d.cr31c_productid1 IS NULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
