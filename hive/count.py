from pyspark.sql import SparkSession

# Create SparkSession.
spark = SparkSession.builder \
.appName("Check Hive") \
.config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
.enableHiveSupport() \
.getOrCreate()

spark.sql("SHOW TABLES;").show()
df = spark.sql("SELECT * FROM comments")
print(df.count())
