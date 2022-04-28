import re

import pandas as pd
from googleapiclient import errors
from youtube_project.Google import Create_Service

class YouTube:
    def __init__(self, client_secret_file, scopes: list = None):
        self.client_secret_file = client_secret_file
        self.scopes = scopes

    def construct_service_instance(self):
        try:
            API_NAME = 'youtube'
            API_VERSION = 'v3'
            service = Create_Service(self.client_secret_file, API_NAME, API_VERSION, self.scopes)
            return service
        except Exception as e:
            print(e)
            return None

    @staticmethod
    def get_channel_videos_detail(service, channel_id):
        response = service.channels().list(
            part='contentDetails',
            id=channel_id
        ).execute()

        if response['pageInfo']['totalResults'] == 0:
            print('Channel not found.')
            return ''
        else:
            uploaded_playlist_id = response.get('items')[0]['contentDetails']['relatedPlaylists']['uploads']
            # print(response)
            # return uploaded_playlist_id
            try:
                response_playlist_items = service.playlistItems().list(
                    part='contentDetails',
                    playlistId=uploaded_playlist_id,
                    maxResults=50
                ).execute()

                playlistItems = response_playlist_items['items']
                nextPageToken = response_playlist_items.get('nextPageToken')

                while nextPageToken:
                    response_playlist_items = service.playlistItems().list(
                        part='contentDetails',
                        playlistId=uploaded_playlist_id,
                        maxResults=50,
                        pageToken=nextPageToken
                    ).execute()

                    playlistItems.extend(response_playlist_items['items'])
                    nextPageToken = response_playlist_items.get('nextPageToken')

                videos = tuple(v['contentDetails'] for v in playlistItems)
                videos_info = []

                for batch_num in range(0, len(videos), 50):
                    video_batch = videos[batch_num: batch_num + 50]

                    response_videos = service.videos().list(
                        id=','.join(list(map(lambda v: v['videoId'], video_batch))),
                        part='snippet,contentDetails,statistics',
                        maxResults=50
                    ).execute()

                    videos_info.extend(response_videos['items'])
                return videos_info

            except errors.HttpError:
                print('Channel has 0 videos')
                return ''
            except Exception as e:
                print(e)

    @staticmethod
    def convert_duration(duration):
        try:
            h = int(re.search('\d+H', duration)[0][:-1]) * 60**2 if re.search('\d+H', duration) else 0 #hour
            m = int(re.search('\d+H', duration)[0][:-1]) * 60 if re.search('\d+H', duration) else 0  #minute
            s = int(re.search('\d+H', duration)[0][:-1]) if re.search('\d+H', duration) else 0  #second
            return h+m+s
        except Exception as e:
            print(e)
            return 0

if __name__ == '__main__':
    CLIENT_SECRET_FILE = 'client_secret_2.json'
    SCOPES = ['https://www.googleapis.com/auth/youtube']
    yt = YouTube(CLIENT_SECRET_FILE, SCOPES)
    service = yt.construct_service_instance()

    channel_id = 'UC3h_EmIijXlx57TJXobrBFA'
    response = yt.get_channel_videos_detail(service, channel_id)

    rows = []
    for video in response:
        rows.append(
            [
                video['id'],
                video['snippet']['title'],
                yt.convert_duration(video['contentDetails']['duration']),
                video['snippet']['publishedAt'][:-1],
                int(video['statistics']['viewCount']) if video['statistics'].get('viewCount') else 0,
                int(video['statistics']['likeCount']) if video['statistics'].get('likeCount') else 0,
                int(video['statistics']['commentCount']) if video['statistics'].get('commentCount') else 0,
                'https://www.youtube.com/watch?v={0}'.format(video['id'])
            ]
        )
    cols = ['video_id', 'video_name', 'duration_seconds', 'published_at', 'view_count', 'like_count', 'comment_count', 'url']
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv('response_dump.csv', index=False)