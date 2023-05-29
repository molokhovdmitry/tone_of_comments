"""
MIT License

Copyright (c) 2023 molokhovdmitry

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from google.protobuf.json_format import MessageToDict

from kafka.consumer import consume_loop
from kafka.comments_pb2 import CommentList


# Create SparkSession.
spark = SparkSession.builder \
.appName("Comment Saver") \
.config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
.config("hive.metastore.uris", "thrift://localhost:9083") \
.enableHiveSupport() \
.getOrCreate()

spark.sql("SHOW TABLES;").show()

def save_to_hive(msg):
    """Parse protobuf message and save comments to Hive."""
    comment_list = CommentList()
    comment_list.ParseFromString(msg.value())
    comment_list = MessageToDict(comment_list, preserving_proto_field_name=True)

    # Comments to dataframe.
    rows = []
    emotions = ["anger", "anticipation", "disgust", "fear", "joy", "love",
                "optimism", "hopeless", "sadness", "surprise", "trust"]
    for comment in comment_list["comments"]:
        for emotion in emotions:
            if emotion not in comment.keys():
                comment[emotion] = False
        rows.append(comment)
    df = spark.createDataFrame(rows)

    # Convert `date` column type to timestamp.
    df = df.withColumn('date', to_timestamp('date', 'yyyy-MM-dd HH:mm:ss'))

    # Order columns.
    columns = [
        "comment_id", "video_id", "channel_id", "text", "date"
    ] + emotions
    df = df.select(columns)

    # Save to Hive.
    if spark.catalog.tableExists("comments"):
        old_df = spark.sql("SELECT * FROM comments")
        old_count = old_df.count()
        df = old_df.union(df).dropDuplicates().subtract(old_df)
        df.write.insertInto("comments")
        count = spark.sql("SELECT * FROM comments").count()
    else:
        count = df.count()
        df.write.mode('overwrite').saveAsTable("comments")

    # Info.
    try:
        print(f"Got {len(comment_list['comments'])}. " +
              f"Previous: {old_count}. Current: {count}. " +
              f"Saved {count - old_count}.")
    except:
        print(f"Got {len(comment_list['comments'])} comment(s). " +
              f"Saved {count} comment(s).")


if __name__ == "__main__":
    consume_loop(["emotions"], save_to_hive)
