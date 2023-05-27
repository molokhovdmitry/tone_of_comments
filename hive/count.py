from pyspark.sql import SparkSession

# Create SparkSession.
spark = SparkSession.builder \
.appName("Check Hive") \
.config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
.config("hive.metastore.uris", "thrift://localhost:9083") \
.enableHiveSupport() \
.getOrCreate()

spark.sql("SHOW TABLES;").show()
df = spark.sql("SELECT * FROM comments")
print(df.count())
