import json

from confluent_kafka import Producer
import socket


conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def produce_comments(comments):
    video_id = list(comments.values())[0]['video_id']
    comments = json.dumps(comments)
    producer.produce('comments', key=video_id, value=comments, callback=acked)
    producer.poll(1)

if __name__ == '__main__':
    from dags.api import get_comments
    comments = get_comments('a7NdLrS63nc')
    produce_comments(comments)
