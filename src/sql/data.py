from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark.sql("""
CREATE TEMPRARY VIEW vw_device
USING org.apache.spark.sql.json 
OPTIONS (path "/spark-series/docs/device/*.json")
""")

spark.sql("""
CREATE TEMPRARY VIEW vw_subscription
USING org.apache.spark.sql.json
OPTIONS (path "/spark-series/docs/subscription/*.json")
""")

print(spark.catalog.listTables())
