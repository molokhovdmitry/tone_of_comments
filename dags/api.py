"""This file is for YouTube api functions."""

import requests

from dags.api_key import API_KEY
from kafka.comments_pb2 import CommentList

def get_comments(video_id):
    """Gets all `commentThreads` from a YouTube video."""
    # Get comments from the first page.
    response = get_response(video_id, max_results=100)
    comment_list = response_to_comments(response)

    # Get comments from the other pages.
    while 'nextPageToken' in response.keys():
        response = get_response(video_id, page_token=response['nextPageToken'])
        comment_list.update(response_to_comments(response))
    
    # Convert `comments` dict to `CommentList` protobuf.
    comment_list = dict_to_protobuf(comment_list).SerializeToString()

    return comment_list

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
    """Converts JSON response to `comments` dict."""
    comment_list = {}
    for comment in response['items']:
        comment = comment['snippet']['topLevelComment']
        try:
            comment_list[comment['id']] = {
                    'video_id': comment['snippet']['videoId'],
                    'channel_id': comment['snippet']['authorChannelId']['value'],
                    'text': comment['snippet']['textOriginal'],
                    'date': comment['snippet']['updatedAt'].replace('T', ' ')[:-1],
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


if __name__ == '__main__':
    video_id = '_VB39Jo8mAQ'
    comments = get_comments(video_id)
    comments = [comments[comment_id]['text']for comment_id in comments]
    import numpy as np
    print(np.array(comments))
