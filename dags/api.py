"""This file is for YouTube api functions."""

import requests
from dags.api_key import API_KEY

def get_comments(video_id):
    """Gets all `commentThreads` from a YouTube video."""
    # Get comments from the first page.
    response = get_response(video_id, max_results=50)
    comments = response_to_comments(response)

    # Get comments from the other pages.
    while 'nextPageToken' in response.keys():
        response = get_response(video_id, page_token=response['nextPageToken'])
        comments.update(response_to_comments(response))
    return comments

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
    """Converts JSON response to `comments` dictionary."""
    comments = {}
    for comment in response['items']:
        comment = comment['snippet']['topLevelComment']
        comments[comment['id']] = {
                'video_id': comment['snippet']['videoId'],
                'channel_id': comment['snippet']['authorChannelId']['value'],
                'text': comment['snippet']['textOriginal'],
                'date': comment['snippet']['updatedAt'].replace('T', ' ')[:-1],
            }
    return comments

if __name__ == '__main__':
    video_id = '_VB39Jo8mAQ'
    comments = get_comments(video_id)
    comments = [comments[comment_id]['text']for comment_id in comments]
    import numpy as np
    print(np.array(comments))
