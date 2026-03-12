# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
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

# PySpark
import com.microsoft.spark.fabric  # available in Fabric runtime
df = spark.read.synapsesql("T_1 SQL.dbo.Test")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
