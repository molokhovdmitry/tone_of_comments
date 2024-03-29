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
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType

from spanemo.load_model import model, preprocessor, tokenizer, device

# Create a Spark session.
spark = SparkSession.builder \
    .appName("Model") \
    .getOrCreate()

# Use broadcasting to share the model across workers.
broadcast = {
    'model': spark.sparkContext.broadcast(model),
    'preprocessor': spark.sparkContext.broadcast(preprocessor),
    'tokenizer': spark.sparkContext.broadcast(tokenizer),
    'device': spark.sparkContext.broadcast(device)
}

# Define the Kafka configuration.
kafka_conf = {"kafka.bootstrap.servers": "localhost:9092"}

# Create a Kafka DataFrame using the Spark session.
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_conf["kafka.bootstrap.servers"]) \
    .option("subscribe", "comments") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() \

df = df.selectExpr("CAST(key AS STRING) as key",
                   "CAST(value AS BINARY) as value")

def predict(data):
    """Prediction UDF."""
    import torch

    from spanemo.inference import preprocess
    from kafka.comments_pb2 import CommentList

    # Load the broadcasted values.
    model = broadcast['model'].value
    preprocessor = broadcast['preprocessor'].value
    tokenizer = broadcast['tokenizer'].value
    device = broadcast['device'].value

    # Read data from message.
    comment_list = CommentList()
    comment_list.ParseFromString(bytes(data))

    # Get texts from `comments` protobuf and make predictions.
    text_values = [comment.text for comment in comment_list.comments]
    data_loader = preprocess(text_values, preprocessor, tokenizer)
    with torch.no_grad():
        for i, batch in enumerate(data_loader):
            if i == 0:
                preds = list(model(batch, device)[1])
            else:
                preds += list(model(batch, device)[1])

    # Fill `comment_list` with emotions.
    emotions = ["anger", "anticipation", "disgust", "fear", "joy",
                "love", "optimism", "hopeless", "sadness", "surprise", "trust"]
    for comment, pred in zip(comment_list.comments, preds):
        for emotion in emotions:
            setattr(comment, emotion, bool(pred[emotions.index(emotion)]))

    return comment_list.SerializeToString()

# Apply UDF.
prediction_udf = udf(lambda data: predict(data), BinaryType())

# Create a Kafka Producer for `emotions` topic.
df.select(df.key, prediction_udf(df.value).alias('value')) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "emotions") \
    .option("checkpointLocation", "~/projects/tone_of_comments/spark/checkpoints") \
    .start() \
    .awaitTermination()
