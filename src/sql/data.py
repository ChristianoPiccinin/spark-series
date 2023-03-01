from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark.sql("""
CREATE TEMPORARY VIEW vw_device
USING org.apache.spark.sql.json 
OPTIONS (path "C:/git/spark-series/docs/file/device/*.json")
""")

spark.sql("""
CREATE TEMPORARY VIEW vw_subscription
USING org.apache.spark.sql.json
OPTIONS (path "C:/git/spark-series/docs/file/device/*.json")
""")

print(spark.catalog.listTables())


spark.sql(""" SELECT * FROM vw_device LIMIT 10""").show()
spark.sql(""" SELECT * FROM vw_subscription LIMIT 10""").show()

join_datasets = spark.sql("""
                          SELECT dev.user_id,
                                 dev.model,
                                 dev.platform, 
                                 subs.payment_method, 
                                 subs.plan
                            FROM vw_device AS dev
                            INNER JOIN vw_subscription AS subs
                            ON dev.user_id = subs.user_id
                            LIMIT 10;
                          """)

join_datasets.show()
join_datasets.printSchema()
join_datasets.count()