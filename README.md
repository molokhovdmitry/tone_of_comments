# Tone of Comments

## About
### Introduction
This is a pet project I am making for the purpose of learning and gaining practical experience with **Spark**, **Kafka**, **Hive**, and **Airflow**. Its main objective is to predict the sentiment of comments posted on YouTube videos.

By using the functionality of **Kafka** and **Spark**, the project can be extended to use multiple prediction models and message sources simultaneously.

### How it works
**FastAPI** web application gets comments from API and produces them into a `comments` **Kafka** topic as a **protobuf** message and waits for predictions in `emotions` topic using **Confluent Kafka** library. **Spark Streaming** application loads the comments from `comments` topic, makes predictions with a UDF and produces them into `emotions` topic. Web application loads the predictions and visualizes them. Predictions from `emotions` topic are also saved to a **Hive** table with a separate script. Web application, Spark Streaming application and a script for Hive are launched by `start_app` **Airflow** DAG.

### Model Training
Training is done by `train` **Airflow** DAG. **Weights & Biases** is used to visualize the training process, store the model and dataset artifacts.

[Weights & Biases Project Page](https://wandb.ai/molokhovdmitry/tone_of_comments)

Training DAG graph:
![Training DAG graph](images/train_dag.png)

`train_model` task preprocesses the data and trains the model, logging the metrics to **Weights & Biases** in the process. Uses early stopping to stop the training and saves the model with the highest validation score to **Weights & Biases** with `latest` alias.

`test_model` task loads the model with `latest` alias and loads the F1 score of the model with `best` alias if it exists. It evaluates the model on `test` set and updates model's metadata with the scores on **Weights & Biases**. If the F1 score of evaluated model is better than that of the `best` model, it also gets the `best` alias.

`trigger_self` task just restarts the DAG for continuous training.

### Model Application
Spark streaming application loads the model from **Weights & Biases** project with `best` alias and uses it for inference. Project could be extended to use multiple spark applications.

Currently uses **SpanEmo** model to predict emotions of the comments.

[GitHub](https://github.com/hasanhuz/SpanEmo), [Paper](https://www.aclweb.org/anthology/2021.eacl-main.135.pdf)

## Installation
## Bugs and Issues