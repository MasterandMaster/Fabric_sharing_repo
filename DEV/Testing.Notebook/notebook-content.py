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

querystring = {
    "query": "Phone",
    "page": "1",
    "country": "US",
    "sort_by": "RELEVANCE",
    "product_condition": "ALL",
    "is_prime": "false",
    "deals_and_discounts": "NONE"
}

headers = {
	"x-rapidapi-key": "3735f1ae14msh00b1497832a56c6p14d8eajsnf436c224fca6",
	"x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

# =========================
# Step 4: API Call
# =========================
response = requests.request("GET", url, headers=headers, params=querystring)
response.raise_for_status()   # fails fast if API fails

# =========================
# Step 5: Parse JSON
# =========================
json_data = response.json()

# Validate expected structure
products = json_data["data"]["products"]  # MUST be a list

# =========================
# Step 6: Create Spark DataFrame
# =========================
df_raw = spark.createDataFrame(products)


df_clean = df_raw.select(
    col("asin"),
    col("product_title"),
    col("product_price").alias("price"),
    col("product_star_rating").alias("rating"),
    col("product_num_ratings").alias("rating_count"),
    col("product_url")
)

df_clean.show(truncate=False)


df_clean.write \
    .mode("append") \
    .format("delta") \
    .save("Tables/bronze_amazon_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
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
    .mode("append") \
    .format("delta") \
    .save("Tables/bronze_amazon_products")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import requests
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create Spark session
spark = SparkSession.builder \
    .appName("AmazonAPIDataExtraction") \
    .getOrCreate()

# Common headers for all requests
headers = {
    "x-rapidapi-key": "3735f1ae14msh00b1497832a56c6p14d8eajsnf436c224fca6",
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

# Define all API endpoints and their parameters
api_requests = [
    {
        "name": "search_products",
        "url": "https://real-time-amazon-data.p.rapidapi.com/search",
        "params": {"query": "Phone", "page": "1", "country": "US", "sort_by": "RELEVANCE",
                   "product_condition": "ALL", "is_prime": "false", "deals_and_discounts": "NONE"}
    },
    {
        "name": "products_by_category",
        "url": "https://real-time-amazon-data.p.rapidapi.com/products-by-category",
        "params": {"category_id": "281407", "page": "1", "country": "US", "sort_by": "RELEVANCE",
                   "product_condition": "ALL", "is_prime": "false", "deals_and_discounts": "NONE"}
    },
    {
        "name": "product_details",
        "url": "https://real-time-amazon-data.p.rapidapi.com/product-details",
        "params": {"asin": "B07ZPKBL9V", "country": "US"}
    },
    {
        "name": "product_reviews",
        "url": "https://real-time-amazon-data.p.rapidapi.com/product-reviews",
        "params": {"asin": "B07ZPKN6YR", "country": "US", "page": "1", "sort_by": "TOP_REVIEWS",
                   "star_rating": "ALL", "verified_purchases_only": "false",
                   "images_or_videos_only": "false", "current_format_only": "false"}
    },
    {
        "name": "seller_profile",
        "url": "https://real-time-amazon-data.p.rapidapi.com/seller-profile",
        "params": {"seller_id": "A02211013Q5HP3OMSZC7W", "country": "US"}
    },
    {
        "name": "seller_products",
        "url": "https://real-time-amazon-data.p.rapidapi.com/seller-products",
        "params": {"seller_id": "A02211013Q5HP3OMSZC7W", "country": "US", "page": "1", "sort_by": "RELEVANCE"}
    },
    {
        "name": "best_sellers",
        "url": "https://real-time-amazon-data.p.rapidapi.com/best-sellers",
        "params": {"category": "software", "type": "BEST_SELLERS", "page": "1", "country": "US"}
    },
    {
        "name": "deal_products",
        "url": "https://real-time-amazon-data.p.rapidapi.com/deal-products",
        "params": {"country": "US", "sort_by": "FEATURED", "page": "1"}
    },
    {
        "name": "product_category_list",
        "url": "https://real-time-amazon-data.p.rapidapi.com/product-category-list",
        "params": {"country": "US"}
    }
]

# Loop through each API request, fetch data, convert to Spark DataFrame and save as JSON
for api in api_requests:
    response = requests.get(api["url"], headers=headers, params=api["params"])
    
    if response.status_code == 200:
        data = response.json()
        
        # If data is a list, convert each item to Row, else wrap in a list
        if isinstance(data, list):
            rows = [Row(**item) if isinstance(item, dict) else Row(data=item)]
        else:
            rows = [Row(**data) if isinstance(data, dict) else Row(data=data)]
        
        df = spark.createDataFrame(rows)
        output_path = f"abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/API_files{api['name']}.json"  # Change this path as needed
        df.write.mode("overwrite").json(output_path)
        print(f"{api['name']} saved to {output_path}")
    else:
        print(f"Failed to fetch {api['name']}. Status Code: {response.status_code}")

spark.stop()




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import requests
from pyspark.sql.functions import col

# Use pre-existing spark session
# spark = SparkSession.builder.getOrCreate()  # Not needed in Fabric

url = "https://real-time-amazon-data.p.rapidapi.com/search"

querystring = {
    "query": "Phone",
    "page": "1",
    "country": "US",
    "sort_by": "RELEVANCE",
    "product_condition": "ALL",
    "is_prime": "false",
    "deals_and_discounts": "NONE"
}

headers = {
    "x-rapidapi-key": "3735f1ae14msh00b1497832a56c6p14d8eajsnf436c224fca6",
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
response.raise_for_status()
json_data = response.json()

products = json_data["data"]["products"]

df_raw = spark.createDataFrame(products)

df_clean = df_raw.select(
    col("asin"),
    col("product_title"),
    col("product_price").alias("price"),
    col("product_star_rating").alias("rating"),
    col("product_num_ratings").alias("rating_count"),
    col("product_url")
)

df_clean.show(truncate=False)

df_clean.write.mode("overwrite").format("json").save(
    "abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/search"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.read.json("abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/API_filessearch_products.json")
display(df1)

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
spark = SparkSession.builder.appName("RapidAPI_Amazon_PySpark").getOrCreate()

# =========================
# Step 3: API Configuration
# =========================
headers = {
    "x-rapidapi-key": "3735f1ae14msh00b1497832a56c6p14d8eajsnf436c224fca6",
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com",
    "accept": "application/json"
}

# =========================
# Step 4: Define API Endpoints and Query Templates
# =========================
api_endpoints = [
    {
        "name": "search",
        "url": "https://real-time-amazon-data.p.rapidapi.com/search",
        "query": {
            "query": "Phone",
            "page": "1",
            "country": "US",
            "sort_by": "RELEVANCE",
            "product_condition": "ALL",
            "is_prime": "false",
            "deals_and_discounts": "NONE"
        }
    },
    {
        "name": "products_by_category",
        "url": "https://real-time-amazon-data.p.rapidapi.com/products-by-category",
        "query": {
            "category_id": "281407",
            "page": "1",
            "country": "US",
            "sort_by": "RELEVANCE",
            "product_condition": "ALL",
            "is_prime": "false",
            "deals_and_discounts": "NONE"
        }
    },
    {
        "name": "product_details",
        "url": "https://real-time-amazon-data.p.rapidapi.com/product-details",
        "query": {
            "asin": "B07ZPKBL9V",
            "country": "US"
        }
    },
    {
        "name": "product_reviews",
        "url": "https://real-time-amazon-data.p.rapidapi.com/product-reviews",
        "query": {
            "asin": "B07ZPKN6YR",
            "country": "US",
            "page": "1",
            "sort_by": "TOP_REVIEWS",
            "star_rating": "ALL",
            "verified_purchases_only": "false",
            "images_or_videos_only": "false",
            "current_format_only": "false"
        }
    },
    {
        "name": "seller_profile",
        "url": "https://real-time-amazon-data.p.rapidapi.com/seller-profile",
        "query": {
            "seller_id": "A02211013Q5HP3OMSZC7W",
            "country": "US"
        }
    },
    {
        "name": "seller_products",
        "url": "https://real-time-amazon-data.p.rapidapi.com/seller-products",
        "query": {
            "seller_id": "A02211013Q5HP3OMSZC7W",
            "country": "US",
            "page": "1",
            "sort_by": "RELEVANCE"
        }
    },
    {
        "name": "best_sellers",
        "url": "https://real-time-amazon-data.p.rapidapi.com/best-sellers",
        "query": {
            "category": "software",
            "type": "BEST_SELLERS",
            "page": "1",
            "country": "US"
        }
    },
    {
        "name": "deal_products",
        "url": "https://real-time-amazon-data.p.rapidapi.com/deal-products",
        "query": {
            "country": "US",
            "sort_by": "FEATURED",
            "page": "1"
        }
    },
    {
        "name": "product_category_list",
        "url": "https://real-time-amazon-data.p.rapidapi.com/product-category-list",
        "query": {
            "country": "US"
        }
    }
]

# =========================
# Step 5: Fetch Data from All APIs
# =========================
all_data = {}

for api in api_endpoints:
    endpoint_name = api["name"]
    url = api["url"]
    query = api["query"]
    
    try:
        response = requests.get(url, headers=headers, params=query)
        response.raise_for_status()
        json_data = response.json()
        all_data[endpoint_name] = json_data
        print(f"Fetched data from {endpoint_name}")
    except Exception as e:
        print(f"Error fetching {endpoint_name}: {e}")
        all_data[endpoint_name] = []

# =========================
# Step 6: Convert Each API Response to Spark DataFrame and Save
# =========================
for name, data in all_data.items():
    try:
        # Some APIs return lists directly, some nested under 'data' key
        if isinstance(data, dict) and "data" in data:
            records = data["data"]
        else:
            records = data
        
        if isinstance(records, list) and records:
            df = spark.createDataFrame(records)
            df.write.mode("overwrite").json(f"abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/{name}_json")
            print(f"Saved {name} to JSON")
        else:
            print(f"No records found for {name}")
    except Exception as e:
        print(f"Error saving {name}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.read.json("abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/product_category_list_json")
df1.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df12 = spark.read.format('delta').load('abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Tables/product_search')
display(df12)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df12 = spark.read.json('abfss://Shubham_Workspace@onelake.dfs.fabric.microsoft.com/Testing.Lakehouse/Files/Registered_Date.json')
display(12)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

jdbc_url = "jdbc:sqlserver://10.10.2.106:1433;databaseName=Fabric"

connection_props = {
    "user": "sa",
    "password": "training@123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


table_name = "dbo.Seller_Profile"

df_sql = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table=table_name,
        properties=connection_props
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

jdbc_url = "jdbc:sqlserver://10.10.2.106:1433;databaseName=Fabric"

df_sql = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.Seller_Profile")
    .option("user", "sa")
    .option("password", "training@123")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

jdbc_url = (
    "jdbc:sqlserver://10.10.2.106:1433;"
    "databaseName=Fabric;"
    "encrypt=true;"
    "trustServerCertificate=true"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sql = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.Seller_Profile")
    .option("user", "sa")
    .option("password", "training@123")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql -artifact "T_1SQL" -type "Warehouse" -bind df1
# MAGIC Select * from test

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT s.*
# MAGIC FROM stg_customer_master s
# MAGIC LEFT JOIN dim_customer t
# MAGIC ON s.customer_id = t.customer_id
# MAGIC AND t.Iscurrent = 1
# MAGIC WHERE 
# MAGIC     t.customer_id IS NULL
# MAGIC     OR s.hash <> t.hash


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
