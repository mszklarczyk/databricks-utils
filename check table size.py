# Databricks notebook source
from pyspark.sql.functions import round, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("numFiles", IntegerType(), True),
    StructField("sizeMB", FloatType(), True),
    StructField("location", StringType(), True),
])
df = spark.createDataFrame([], schema)

schemas = ["panda_bronze_dev.occ_pax", "panda_bronze_dev.occ_ops"]
table_list = []
for scheme in schemas:
    object_list = spark.catalog.listTables(scheme)
    table_list += ([f"{x.catalog}.{x.namespace[0]}.{x.name}" for x in object_list if x.tableType != "VIEW"])

for table in table_list:
    filenum_df = (spark.sql(f"describe detail {table}")
            .select('name', 
                    'numFiles',
                    (round(col('sizeInBytes')/1024/1024,2)).alias('sizeMB'), 
                    'location')
            )

    df = df.union(filenum_df)
display(df)


