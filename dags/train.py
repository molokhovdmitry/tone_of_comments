import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
        "train",
        schedule=None,
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
) as dag:
        dag.doc_md = """
        This DAG trains and evaluates the model.
        """
        train_model = BashOperator(
            task_id='train_model',
            bash_command="cd ~/projects/tone_of_comments/spanemo && \
                          python train.py \
                          --train-path original_corpus/train.txt \
                          --dev-path original_corpus/dev.txt \
                          --train-batch-size=12 \
                          --eval-batch-size=12"
        )

        # Evaluate the model and update on wandb.
        test_model = BashOperator(
            task_id='test_model',
            bash_command="cd ~/projects/tone_of_comments/spanemo && \
                          python test.py \
                          --test-path original_corpus/test.txt \
                          --model-path wandb"
        )

        # Task for continuous training.
        trigger_self = TriggerDagRunOperator(
                task_id='trigger_self',
                trigger_dag_id='train'
        )

        train_model >> test_model >> trigger_self