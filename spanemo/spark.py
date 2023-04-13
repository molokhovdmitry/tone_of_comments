from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from spanemo.load_model import model, preprocessor, tokenizer, device


# Create a Spark session.
spark = SparkSession.builder \
    .appName("Model") \
    .getOrCreate()

# Use broadcasting to share the model across tasks.
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
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \

df_json = df.selectExpr("CAST(key AS STRING) as key",
                        "CAST(value AS STRING) as value")

def predict(data):
    """Prediction function."""
    import json
    import torch

    from spanemo.inference import preprocess

    # Load the broadcasted values.
    model = broadcast['model'].value
    preprocessor = broadcast['preprocessor'].value
    tokenizer = broadcast['tokenizer'].value
    device = broadcast['device'].value

    # Read data from json string.
    comments = json.loads(data)
    emotions = ["anger", "anticipation", "disgust", "fear", "joy",
                "love", "optimism", "hopeless", "sadness", "surprise", "trust"]
    comments = {
        comment_id: {
                'text': comments[comment_id]['text'],
                'emotions': {emotion: None for emotion in emotions}
            } for comment_id in comments
    }

    # Get texts from `comments` dictionary and make predictions.
    data = [comments[comment_id]['text'] for comment_id in comments]
    data_loader = preprocess(data, preprocessor, tokenizer)
    print("Making predictions.")
    with torch.no_grad():
        for i, batch in enumerate(data_loader):
            if i == 0:
                preds = list(model(batch, device)[1])
            else:
                preds += list(model(batch, device)[1])

    # Fill `comments` with emotions.
    for comment_id, pred in zip(comments, preds):
        for emotion in emotions:
            comment = comments[comment_id]
            comment['emotions'][emotion] = int(pred[emotions.index(emotion)])

    return json.dumps(comments)

prediction_udf = udf(lambda data: predict(data), StringType())

# Create a Kafka Producer for `emotions` topic.
df_json.select(df_json.key, prediction_udf(df_json.value).alias('value')) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "emotions") \
    .option("checkpointLocation", "~/projects/comment_analyzer/spark/checkpoints") \
    .start() \
    .awaitTermination()
