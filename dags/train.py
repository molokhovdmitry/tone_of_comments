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