import sys

from confluent_kafka import Consumer, KafkaError, KafkaException

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': "app",
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)


def basic_consume_loop(consumer, topics):
    running = True
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event.
                    sys.stderr.write(
                        '%% %s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset())
                    )
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def msg_process(msg):
    print(msg)


def find_message(video_id, topics, keep_trying=True):
    """This loop finds specified video's message in specified topics."""
    running = True
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if keep_trying:
                    continue
                else:
                    break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event.
                    sys.stderr.write(
                        '%% %s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset())
                    )
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                key = msg.key()
                if key == video_id.encode():
                    message = msg.value()
                    consumer.close()
                    return message

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


if __name__ == '__main__':
    print(find_message('YeMcTFQhf5c', ['comments'], keep_trying=False))
