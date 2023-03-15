import torch
from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np

from dags.spanemo.model import SpanEmo
from dags.spanemo.inference import choose_model, preprocess
from dags.api import get_comments


app = FastAPI(title="comment_analyzer")


@app.on_event("startup")
def load_model():
    global device
    device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
    if str(device) == 'cuda:0':
        print("Currently using GPU: {}".format(device))
    else:
        print("Currently using CPU")

    global model
    model = SpanEmo()
    path = choose_model()
    model.load_state_dict(torch.load(path))
    model.eval()
    model.to(device)


@app.get("/")
def home():
    return "comment_analyzer"


@app.post("/predict")
def predict(video_id):
    comments = get_comments(video_id)
    comments = [comments[comment_id]['text']for comment_id in comments]
    batch = preprocess(comments)
    with torch.no_grad():
        pred = model(batch, device)[1]
    return {"Response": pred.tolist()}
