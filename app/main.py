import json

from fastapi import FastAPI

from dags.api import get_comments
from kafka.producer import produce_to_topic
from kafka.consumer import find_message

app = FastAPI(title='comment_analyzer')


@app.get('/')
def home():
    return 'comment_analyzer'


@app.post('/predict')
def predict(video_id):
    """Makes predictions."""

    """
    Retrieve predictions from Kafka topic if available,
    else get comments from YouTube and publish to the topic.
    """
    msg = find_message(video_id, ['emotions'], keep_trying=False)
    if msg is None:
        comments = get_comments(video_id)
        produce_to_topic('comments', video_id, comments)
        msg = find_message(video_id, ['emotions'], keep_trying=True)
        comments = json.loads(msg)
    else:
        comments = json.loads(msg)

    return {'Response': comments}
