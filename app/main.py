from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from google.protobuf.json_format import MessageToDict

from api.api import get_comments
from kafka.producer import produce_to_topic
from kafka.consumer import find_message
from kafka.comments_pb2 import CommentList

app = FastAPI(title='comment_analyzer')


app.mount("/static", StaticFiles(directory="app/static"), name="static")


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
    comment_list = CommentList()
    if msg is None:
        try:
            comments = get_comments(video_id)
        except:
            return {'error': 'Invalid ID.'}

        produce_to_topic('comments', video_id, comments)
        msg = find_message(video_id, ['emotions'], keep_trying=True)

    comment_list.ParseFromString(msg)
    comment_list = MessageToDict(comment_list)

    return {'Response': comment_list}
