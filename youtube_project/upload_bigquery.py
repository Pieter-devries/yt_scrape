import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import types
from alive_progress import alive_bar

# Credentials
BIGQUERY_CREDENTIAL = "C:\\Users\\Pieter\\Downloads\\thesis-project-252601-7f024814715e.json"
df = pd.read_csv('C:\\Users\\Pieter\\PycharmProjects\\scrape\\youtube_project\\response_dump.csv')
df['published_at'] = df['published_at'].str.replace('T', ' ')
# print(df['published_at'])


db = create_engine(
    'bigquery://',
    credentials_path=BIGQUERY_CREDENTIAL
)

CREATE_TABLE = """
CREATE TABLE `thesis-project-252601.YoutubeData.scrape_data` (
`video_id` NUMERIC,
`video_name` STRING,
`duration_seconds` NUMERIC,
`published_at` DATETIME,
`view_count` NUMERIC,
`like_count` NUMERIC,
`comment_count` NUMERIC
);
"""
UPLOAD_DATA = """
INSERT INTO `thesis-project-252601.YoutubeData.scrape_data` (
`video_id`,`video_name`,`duration_seconds`,`published_at`,`view_count`,`like_count`,`comment_count`
)
VALUES (
1,'1',1,"2020-01-01 01:01:01",1,1,1
);
"""
QUERY = """
SELECT *  
FROM `thesis-project-252601.YoutubeData.scrape_data`
LIMIT 1
;
"""
DELETE = """
DELETE FROM `thesis-project-252601.YoutubeData.scrape_data` WHERE true;
"""


# pd.read_sql(DELETE, con=db)
# pd.read_sql(UPLOAD_DATA, con=db)
# pd.read_sql(CREATE_TABLE, con=db)
# print(pd.read_sql(QUERY, con=db))

def upload():
    for i in range(1000):
        df.to_sql('YoutubeData.scrape_data', con=db, if_exists='replace', index=False,
                  dtype={
                      'video_id': types.String(),
                      'video_name': types.String(),
                      'duration_seconds': types.Integer(),
                      'published_at': types.DATETIME(),
                      'view_count': types.Integer(),
                      'like_count': types.Integer(),
                      'comment_count': types.Integer(),
                      'url': types.String()
                  }
                  )
        yield


with alive_bar(1000) as bar:
    for i in upload():
        bar()
