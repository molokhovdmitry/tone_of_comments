from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder \
    .appName("test") \
    .master("local[*]") \
    .getOrCreate()

# Create a streaming DataFrame reading from Kafka topic "test"
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load()

# Apply some transformations to the streaming DataFrame
words = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(explode(split("value", " ")).alias("word"))

# Compute word counts
word_counts = words.groupBy("word").count()
print(word_counts)
