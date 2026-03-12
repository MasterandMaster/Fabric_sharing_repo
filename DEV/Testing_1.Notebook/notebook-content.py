# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "94999bf8-01ef-4da5-a715-508f29dbf8a8",
# META       "default_lakehouse_name": "Testing",
# META       "default_lakehouse_workspace_id": "4d148c35-b745-498d-a028-d30a45dc644a",
# META       "known_lakehouses": [
# META         {
# META           "id": "94999bf8-01ef-4da5-a715-508f29dbf8a8"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "b808d42f-9d82-4bd3-b71f-a73130bdc287",
# META       "known_warehouses": [
# META         {
# META           "id": "b808d42f-9d82-4bd3-b71f-a73130bdc287",
# META           "type": "MountedWarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# API 

# CELL ********************

# =========================
# Step 1: Imports
# =========================
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =========================
# Step 2: Spark Session
# =========================
spark = SparkSession.builder.appName("RapidAPI_Amazon_PySpark").getOrCreate()

# =========================
# Step 3: API Configuration
# =========================
url = "https://real-time-amazon-data.p.rapidapi.com/search"

headers = {
    "x-rapidapi-key": "3735f1ae14msh00b1497832a56c6p14d8eajsnf436c224fca6",
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com",
    "accept": "application/json"
}

# Query parameters template
query_template = {
    "query": "Phone",
    "page": "1",  # will be updated in loop
    "country": "US",
    "sort_by": "RELEVANCE",
    "product_condition": "ALL",
    "is_prime": "false",
    "deals_and_discounts": "NONE"
}

# =========================
# Step 4: Fetch Multiple Pages (to get 200 records)
# =========================
all_products = []
records_needed = 200
per_page = 50  # assume API returns 50 per page
pages_needed = (records_needed + per_page - 1) // per_page  # ceil division

for page in range(1, pages_needed + 1):
    querystring = query_template.copy()
    querystring["page"] = str(page)

    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()

    json_data = response.json()
    products = json_data["data"]["products"]

    if not products:
        break  # no more products returned

    all_products.extend(products)

    if len(all_products) >= records_needed:
        break

# Trim to exactly 200
all_products = all_products[:records_needed]

# =========================
# Step 5: Create Spark DataFrame
# =========================
df_raw = spark.createDataFrame(all_products)

# =========================
# Step 6: Select / Flatten Fields
# =========================
df_clean = df_raw.select(
    col("asin"),
    col("product_title"),
    col("product_price").alias("price"),
    col("product_star_rating").alias("rating"),
    col("product_num_ratings").alias("rating_count"),
    col("product_url")
)

df_clean.show(truncate=False)

# =========================
# Step 7: Save to Delta (Bronze Layer)
# =========================
df_clean.write \
    .mode("overwrite") \
    .format("json") \
    .save("abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/Product_Search")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================
# Step 1: Imports
# =========================
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =========================
# Step 2: Spark Session
# =========================
spark = SparkSession.builder.appName("Amazon_Category_Products").getOrCreate()

# =========================
# Step 3: API Configuration
# =========================
url = "https://real-time-amazon-data.p.rapidapi.com/products-by-category"

headers = {
    "x-rapidapi-key": "3735f1ae14msh00b1497832a56c6p14d8eajsnf436c224fca6",
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

# Query template
query_template = {
    "category_id": "281407",
    "page": "1",  # will be updated in loop
    "country": "US",
    "sort_by": "RELEVANCE",
    "product_condition": "ALL",
    "is_prime": "false",
    "deals_and_discounts": "NONE"
}

# =========================
# Step 4: Fetch Multiple Pages (200 records)
# =========================
all_products = []
records_needed = 200
per_page = 50  # assume API returns 50 per page
pages_needed = (records_needed + per_page - 1) // per_page  # ceil division

for page in range(1, pages_needed + 1):
    querystring = query_template.copy()
    querystring["page"] = str(page)

    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()

    json_data = response.json()
    products = json_data.get("data", {}).get("products", [])

    if not products:
        break  # no more products returned

    all_products.extend(products)

    if len(all_products) >= records_needed:
        break

# Trim to exactly 200
all_products = all_products[:records_needed]

# =========================
# Step 5: Create Spark DataFrame
# =========================
df_raw = spark.createDataFrame(all_products)

# =========================
# Step 6: Select / Flatten Fields
# =========================
df_clean = df_raw.select(
    col("asin"),
    col("product_title"),
    col("product_price").alias("price"),
    col("product_star_rating").alias("rating"),
    col("product_num_ratings").alias("rating_count"),
    col("product_url")
)

df_clean.show(truncate=False)

# =========================
# Step 7: Save to Lakehouse (JSON format)
# =========================
df_clean.write \
    .mode("overwrite") \
    .format("json") \
    .save("abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/Category_Products")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================
# Step 1: Imports
# =========================
import requests
from pyspark.sql import SparkSession

# =========================
# Step 2: Spark Session
# =========================
spark = SparkSession.builder \
    .appName("Amazon_Seller_Profile") \
    .getOrCreate()

# =========================
# Step 3: API Configuration
# =========================
url = "https://real-time-amazon-data.p.rapidapi.com/seller-profile"

headers = {
    "x-rapidapi-key": "3735f1ae14msh00b1497832a56c6p14d8eajsnf436c224fca6",  # move to Key Vault in prod
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com",
    "accept": "application/json"
}

querystring = {
    "seller_id": "A02211013Q5HP3OMSZC7W",
    "country": "US"
}

# =========================
# Step 4: Call API
# =========================
response = requests.get(url, headers=headers, params=querystring)
response.raise_for_status()

json_data = response.json()

seller = json_data.get("data", {})

# =========================
# Step 5: Normalize API Response (Schema Safe)
# =========================
record = {
    "seller_id": seller.get("seller_id"),
    "seller_name": seller.get("name"),
    "seller_rating": seller.get("rating"),
    "rating_count": seller.get("ratings_total"),
    "positive_feedback_pct": seller.get("ratings_total_percentage"),
    "storefront_url": seller.get("store_link"),
    "business_name": seller.get("business_name"),
    "business_address": seller.get("business_address"),
    "seller_domain": seller.get("domain"),
    "seller_phone": seller.get("phone"),
    "country": seller.get("country")
}

# =========================
# Step 6: Create Spark DataFrame
# =========================
df_clean = spark.createDataFrame([record])

# =========================
# Step 7: Validate
# =========================
df_clean.printSchema()
df_clean.show(truncate=False)

# =========================
# Step 8: Save to Lakehouse (Bronze – JSON)
# =========================
df_clean.write \
    .mode("overwrite") \
    .format("json") \
    .save(
        "abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/"
        "Testing.Lakehouse/Files/Seller_Profile"
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================
# Step 1: Imports
# =========================
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType
)

# =========================
# Step 2: Spark Session
# =========================
spark = SparkSession.builder \
    .appName("Amazon_Product_Details") \
    .getOrCreate()

# =========================
# Step 3: API Configuration
# =========================
url = "https://real-time-amazon-data.p.rapidapi.com/product-details"

headers = {
    "x-rapidapi-key": "3735f1ae14msh00b1497832a56c6p14d8eajsnf436c224fca6",  # secure in prod
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com",
    "accept": "application/json"
}

querystring = {
    "asin": "B07ZPKBL9V",
    "country": "US"
}

# =========================
# Step 4: Call API
# =========================
response = requests.get(url, headers=headers, params=querystring)
response.raise_for_status()

product = response.json().get("data", {})

# =========================
# Step 5: Normalize API Response
# =========================
record = {
    "asin": product.get("asin"),
    "product_title": product.get("product_title"),
    "price": (
        product.get("product_price", {}).get("value")
        if isinstance(product.get("product_price"), dict)
        else product.get("product_price")
    ),
    "rating": product.get("product_star_rating"),
    "rating_count": product.get("product_num_ratings"),
    "brand": product.get("brand"),
    "availability_status": product.get("availability_status"),
    "product_description": (
        " ".join(product.get("product_description"))
        if isinstance(product.get("product_description"), list)
        else product.get("product_description")
    ),
    "product_url": product.get("product_url"),
    "country": querystring["country"]
}

# =========================
# Step 6: DEFINE SCHEMA (CRITICAL STEP)
# =========================
schema = StructType([
    StructField("asin", StringType(), True),
    StructField("product_title", StringType(), True),
    StructField("price", StringType(), True),         # keep string at Bronze
    StructField("rating", StringType(), True),
    StructField("rating_count", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("availability_status", StringType(), True),
    StructField("product_description", StringType(), True),
    StructField("product_url", StringType(), True),
    StructField("country", StringType(), True)
])

# =========================
# Step 7: Create Spark DataFrame (NO INFERENCE)
# =========================
df_clean = spark.createDataFrame([record], schema=schema)

# =========================
# Step 8: Validate
# =========================
df_clean.printSchema()
df_clean.show(truncate=False)

# =========================
# Step 9: Save to Lakehouse (Bronze – JSON)
# =========================
df_clean.write \
    .mode("overwrite") \
    .format("json") \
    .save(
        "abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/"
        "Testing.Lakehouse/Files/Product_Details/"
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.read.json('abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/Product_Search')
df2 = spark.read.json('abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/Category_Products')
df3 = spark.read.json('abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/Product_Details')
df4 = spark.read.json('abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/Seller_Profile')

df1.write.mode('overwrite').format('delta').saveAsTable('Product_Search')
df2.write.mode('overwrite').format('delta').saveAsTable('Category_Products')
df3.write.mode('overwrite').format('delta').saveAsTable('Product_Details')
df4.write.mode('overwrite').format('delta').saveAsTable('Seller_Profile')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PySpark
import com.microsoft.spark.fabric  # available in Fabric runtime
df = spark.read.synapsesql("T_1SQL.dbo.Test")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
