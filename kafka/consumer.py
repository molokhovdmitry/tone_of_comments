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

import sys

from confluent_kafka import Consumer, KafkaError, KafkaException


def consume_loop(topics, msg_process):
    """Modified consume loop from documentation."""
    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': "spanemo",
            'enable.auto.commit': False,
            'auto.offset.reset': 'latest'}
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
