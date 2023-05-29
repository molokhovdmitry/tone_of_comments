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

"""This file is for YouTube api interactions."""

import requests
import hashlib
import asyncio

from api.api_key import API_KEY
from kafka.comments_pb2 import CommentList

async def get_comments(video_id, max_comments=1000):
    """Yields all `commentThreads` from a YouTube video in batches."""

    # Get comments from the first page.
    response = get_response(video_id, max_results=100)
    comment_list = response_to_comments(response)

    # Get comments from the other pages.
    while 'nextPageToken' in response.keys():
        response = get_response(video_id, page_token=response['nextPageToken'])
        comment_list.update(response_to_comments(response))

        if len(comment_list) >= max_comments - 100:
            yield serialize_and_hash(comment_list)
            comment_list = {}
            await asyncio.sleep(0)

    if comment_list:
        yield serialize_and_hash(comment_list)
        await asyncio.sleep(0)


def serialize_and_hash(comment_list):
    """Converts `comment_list` dict to a protobuf, serializes it and hashes."""
    comment_list = dict_to_protobuf(comment_list).SerializeToString()
    key = hashlib.sha1(comment_list).hexdigest()
    return comment_list, key


def get_response(video_id, page_token=None, max_results=100):
    """Gets the response from YouTube API and converts it to JSON."""
    url = 'https://youtube.googleapis.com/youtube/v3/commentThreads'
    payload = {
        'videoId': video_id,
        'key': API_KEY,
        'maxResults': max_results,
        'part': 'snippet',
        'pageToken': page_token,
    }
    response = requests.get(url, params=payload)
    return response.json()


def response_to_comments(response):
    """Converts JSON response to `comment_list` dict."""
    comment_list = {}
    for comment in response['items']:
        comment = comment['snippet']['topLevelComment']
        channel_id = comment['id']
        comment = comment['snippet']
        try:
            comment_list[channel_id] = {
                    'video_id': comment['videoId'],
                    'channel_id': comment['authorChannelId']['value'],
                    'text': comment['textOriginal'],
                    'date': comment['updatedAt'].replace('T', ' ')[:-1],
                }
        except Exception as e:
            print(f"Error: {e}\nComment: {comment}")
            continue

    return comment_list


def dict_to_protobuf(comment_dict):
    """Converts `comments` dict to `CommentList` protobuf."""
    comment_list = CommentList()
    for comment_id, comment_data in comment_dict.items():
        comment = comment_list.comments.add()
        comment.comment_id = comment_id
        comment.video_id = comment_data["video_id"]
        comment.channel_id = comment_data["channel_id"]
        comment.text = comment_data["text"]
        comment.date = comment_data["date"]

    return comment_list
