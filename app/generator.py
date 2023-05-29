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

import asyncio
import json

from google.protobuf.json_format import MessageToDict

from api.api import get_comments
from kafka.producer import produce_to_topic
from kafka.consumer import find_message
from kafka.comments_pb2 import CommentList


async def generate_preds(video_id):
    """
    Runs all the tasks to load comments, produce and receive
    Kafka messages to get predictions for batches in parallel.
    Yields comments with predictions.
    """
    # Create queues.
    comments_queue = asyncio.Queue(maxsize=5)
    wait_queue = asyncio.Queue(maxsize=5)

    # Create tasks
    load_task = asyncio.create_task(load_comments(video_id, comments_queue, wait_queue))
    loop = asyncio.get_running_loop()
    produce_task = asyncio.create_task(produce(comments_queue, wait_queue, loop))
    stop_task = asyncio.Event()
    wait_task = wait_for_preds(wait_queue, loop)

    # Receive and yield predictions from `wait_task`.
    full_comment_list = {}
    async for comment_list in wait_task:
        # Stop if done.
        if comment_list == None:
            stop_task.set()
            print(f"`wait_task` stopped")
            break
        
        # Concat with previous preds.
        if not full_comment_list:
            full_comment_list = comment_list
        else:
            full_comment_list['comments'] += comment_list['comments']
        yield json.dumps({'Response': full_comment_list})

    # Wait for queues to be processed and stop tasks.
    await comments_queue.join()
    await wait_queue.join()
    load_task.cancel()
    produce_task.cancel()

    # Wait for completion of tasks.
    await asyncio.gather(load_task, produce_task, return_exceptions=True)


async def load_comments(video_id, comments_queue, wait_queue):
    """
    Loads the comments into the `comments_queue`. Sends `None` to
    `wait_queue` when done to stop the `wait_for_preds` task.
    """
    async for comments, key in get_comments(video_id):
        await comments_queue.put((comments, key))
    await asyncio.sleep(5)
    print("`load_comments` done")
    await wait_queue.put(None)

async def produce(comments_queue, wait_queue, loop):
    """
    Loads comments from `comments_queue`. If the comments' predictions are
    not in the `emotions` topic, they are produced into the `comments` topic
    and placed in the `wait_queue`. Otherwise, they are just placed in the
    `wait_queue`.
    """
    while True:
        # Get comments.
        comments, key = await comments_queue.get()

        # Try to find them in `emotions` topic.
        msg = await loop.run_in_executor(
            None, find_message, key, ['emotions'], False
        )
        if msg is None:
            # Produce to `comments` topic and put into `wait_queue`.
            produce_to_topic('comments', key, comments)
            await wait_queue.put(key)
            comments_queue.task_done()
        else:
            # Put comments into `wait_queue`.
            await wait_queue.put(key)
            comments_queue.task_done()

async def wait_for_preds(wait_queue, loop):
    """
    Loads comments' keys from `wait_queue` and waits for predictions to
    appear in `emotions` topic and yields them.
    """

    # Get key.
    while True:
        key = await wait_queue.get()

    # Check if `load_comments` finished.
        if key == None:
            if wait_queue.empty():
                # Yield `None` to stop processed queue.
                wait_queue.task_done()
                yield None
            else:
                # Put `None` back in the queue to stop later.
                wait_queue.task_done()
                await wait_queue.put(None)
                continue

        # Wait for the predictions.
        print(f"Waiting for {key}")
        msg = await loop.run_in_executor(
            None, find_message, key, ['emotions'], True
        )
        print(f"Yielded {key}")
        comment_list = CommentList()
        comment_list.ParseFromString(msg)
        comment_list = MessageToDict(comment_list)
        wait_queue.task_done()

        yield comment_list
