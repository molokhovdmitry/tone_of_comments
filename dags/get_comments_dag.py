import pendulum
import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from api import get_comments

@dag(
    dag_id='get-comments',
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def get_comments():
    create_videos_table = PostgresOperator(
        task_id='create_videos_table',
        postgres_conn_id='pg_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS videos (
                id NUMERIC PRIMARY KEY,
                channel_name TEXT,
                video_name TEXT,
            );""",
    )

    create_comments_table = PostgresOperator(
        task_id='create_comments_table',
        postgres_conn_id='pg_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS comments (
                id NUMERIC PRIMARY KEY,
                video_id NUMERIC FOREIGN KEY REFERENCES videos(id),
                text TEXT,
                date DATETIME,
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
                trust BOOL,
            );""",        
    )

    @task
    def get_and_save_comments():
        comments = get_comments('_VB39Jo8mAQ')