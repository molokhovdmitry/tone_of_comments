import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        "train",
        schedule=None,
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
) as dag:
        dag.doc_md = """
        This DAG trains the model.
        """
        train_the_model = BashOperator(
            task_id='train_the_model',
            bash_command="cd ~/projects/comment_analyzer/dags/spanemo && python train.py --train-path original_corpus/train.txt --dev-path original_corpus/dev.txt --train-batch-size=24 --eval-batch-size=24 && mkdir test"
        )

        train_the_model