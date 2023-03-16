import torch
from fastapi import FastAPI

from dags.spanemo.model import SpanEmo
from dags.spanemo.inference import choose_model, preprocess
from dags.api import get_comments


app = FastAPI(title='comment_analyzer')


@app.on_event('startup')
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


@app.get('/')
def home():
    return 'comment_analyzer'


@app.post('/predict')
def predict(video_id):
    comments = get_comments(video_id)
    emotions = ["anger", "anticipation", "disgust", "fear", "joy",
                "love", "optimism", "hopeless", "sadness", "surprise", "trust"]
    comments = {
        comment_id: {
            'text': comments[comment_id]['text'],
            'emotions': {emotion: None for emotion in emotions}
        } for comment_id in comments
    }
    batch = [comments[comment_id]['text'] for comment_id in comments]
    batch = preprocess(batch)
    with torch.no_grad():
        preds = model(batch, device)[1]
    
    for comment_id, pred in zip(comments, preds):
        for emotion in emotions:
            comment = comments[comment_id]
            comment['emotions'][emotion] = int(pred[emotions.index(emotion)])
    return {'Response': comments}
