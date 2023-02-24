#import libraries and init spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# load data 
df_device = spark.read.json("C:/git/spark-series/docs/file/device/device_2022_6_7_19_39_24.json")

# show data
df_device.show()

# print schema
df_device.printSchema()

# print column names
print(df_device.columns)

# row 
print(df_device.count())

# select column
df_device.select("id","user_id").show()

df_device.selectExpr("id","user_id as user").show()

# filter
df_device.filter(df_device["manufacturer"] == "Lenovo").show()

# group by 
df_device.groupBy("manufacturer").count().show()