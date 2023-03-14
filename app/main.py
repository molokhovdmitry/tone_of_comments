from fastapi import FastAPI
from pydantic import BaseModel

from dags.spanemo.inference import predict, preprocess


app = FastAPI(title="comment_analyzer")

"""
@app.on_event("startup")
def load_model():
    raise NotImplementedError
"""


@app.get("/")
def home():
    return "comment_analyzer"


@app.post("/predict")
def predict(text):
    array = preprocess(text)
    pred = predict(array)
    return {"Prediction": pred}
