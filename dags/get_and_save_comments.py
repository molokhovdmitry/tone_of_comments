import pendulum
import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from api import get_comments

@dag(
    dag_id='get-comments',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def get_and_save_comments():
    create_comments_table = PostgresOperator(
        task_id='create_comments_table',
        postgres_conn_id='pg_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS comments (
                id TEXT PRIMARY KEY,
                video_id TEXT,
                channel_id TEXT,
                text TEXT,
                date TIMESTAMP,
                anger BOOL,
                anticipation BOOL,
                disgust BOOL,
                fear BOOL,
                joy BOOL,
                love BOOL,
                optimism BOOL,
                hopeless BOOL,
                sadness BOOL,
                surprise BOOL,
                trust BOOL
            );""",        
    )

    @task
    def get_youtube_comments():
        comments = get_comments('_VB39Jo8mAQ')
        return comments

    @task
    def save_comments(comments: dict):
        postgres_hook = PostgresHook(postgres_conn_id='pg_conn')
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        for comment_id in comments:
            video_id = comments[comment_id]['video_id']
            channel_id = comments[comment_id]['channel_id']
            text = comments[comment_id]['text']
            date = comments[comment_id]['date']
            data = (comment_id, video_id, channel_id, text, date)
            query = """
                INSERT INTO comments (id, video_id, channel_id, text, date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """
            cur.execute(query, data)
        conn.commit()

    comments = get_youtube_comments()
    create_comments_table >> get_youtube_comments() >> save_comments(comments)

get_and_save_comments()
