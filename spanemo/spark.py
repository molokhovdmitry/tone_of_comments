import torch

from ekphrasis.classes.tokenizer import SocialTokenizer
from ekphrasis.classes.preprocessor import TextPreProcessor
from transformers import BertTokenizer

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, concat, collect_list, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from spanemo.model import SpanEmo
from spanemo.inference import choose_model, preprocess


def load_model():
    """Loads the model and preprocessor."""

    # Choose the device and load the model.
    global device
    device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
    if str(device) == 'cuda:0':
        print("Currently using GPU: {}".format(device))
    else:
        print("Currently using CPU")

    global model
    model = SpanEmo()
    path = choose_model()
    model.load_state_dict(torch.load(path))
    model.eval()
    model.to(device)

    # Load the preprocessor.
    global preprocessor
    preprocessor = TextPreProcessor(
        normalize=['url', 'email', 'phone', 'user'],
        annotate={"hashtag", "elongated", "allcaps", "repeated", 'emphasis', 'censored'},
        all_caps_tag="wrap",
        fix_text=False,
        segmenter="twitter_2018",
        corrector="twitter_2018",
        unpack_hashtags=True,
        unpack_contractions=True,
        spell_correct_elong=False,
        tokenizer=SocialTokenizer(lowercase=True).tokenize).pre_process_doc

    global tokenizer
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased', do_lower_case=True)

load_model()


# Create a Spark session
spark = SparkSession.builder \
        .appName("Kafka Consumer") \
        .getOrCreate()

# Define the Kafka configuration
kafka_conf = {"kafka.bootstrap.servers": "localhost:9092"}

# Create a Kafka DataFrame using the Spark session
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
    import json

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

my_udf = udf(lambda data: predict(data), StringType())

# Create a Kafka Producer for `spark` topic.
df_json.select(df_json.key, my_udf(df_json.value).alias('value')) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "emotions") \
    .option("checkpointLocation", "~/projects/comment_analyzer/spark/checkpoints") \
    .start() \
    .awaitTermination()
