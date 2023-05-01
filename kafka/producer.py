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


def produce_to_topic(topic, key, value):
    producer.produce(topic, key=key, value=value, callback=acked)
    producer.poll(1)

if __name__ == '__main__':
    from api.api import get_comments
    video_id = 'r28ime9wMzM'
    comment_list = get_comments(video_id)
    produce_to_topic('comments', video_id, comment_list)
