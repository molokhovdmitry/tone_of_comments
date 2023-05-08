import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
        "start_app",
        schedule=None,
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
) as dag:
        dag.doc_md = """
        This DAG starts the app.
        """
        webserver = BashOperator(
            task_id='webserver',
            bash_command="cd ~/projects/tone_of_comments && \
                          uvicorn app.main:app --reload"
        )

        spark = BashOperator(
            task_id='spark',
            bash_command="cd ~/projects/tone_of_comments && \
                          ./spark.sh "
        )

        hive_saver = BashOperator(
            task_id='hive_saver',
            bash_command="cd ~/projects/tone_of_comments && \
                          python -m hive.save"
        )

        webserver, spark, hive_saver