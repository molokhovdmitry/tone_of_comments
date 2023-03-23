import sys

from confluent_kafka import Consumer, KafkaError, KafkaException


def consume_loop(topics, msg_process):
    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': "spanemo",
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)

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


def find_message(video_id, topics, keep_trying=True):
    """This loop finds specified video's message in specified topics."""

    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': "app",
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)

    running = True
    message = None
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
                    break

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        return message


if __name__ == '__main__':
    print(find_message('EhJAftuGxKA', ['comments'], keep_trying=False))
