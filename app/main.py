# import tracemalloc
import gc

import torch
from fastapi import FastAPI
from ekphrasis.classes.tokenizer import SocialTokenizer
from ekphrasis.classes.preprocessor import TextPreProcessor

from dags.spanemo.model import SpanEmo
from dags.spanemo.inference import choose_model, preprocess
from dags.api import get_comments


app = FastAPI(title='comment_analyzer')
# tracemalloc.start()

@app.on_event('startup')
def load_model():
    # Choose the device and load the model.
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

    # Load the preprocessor.
    global preprocessor
    preprocessor = TextPreProcessor(
        normalize=['url', 'email', 'phone', 'user'],
        annotate={"hashtag", "elongated", "allcaps", "repeated", 'emphasis', 'censored'},
        all_caps_tag="wrap",
        fix_text=False,
        segmenter="twitter_2018",
        corrector="twitter_2018",
        unpack_hashtags=True,
        unpack_contractions=True,
        spell_correct_elong=False,
        tokenizer=SocialTokenizer(lowercase=True).tokenize).pre_process_doc

@app.get('/')
def home():
    return 'comment_analyzer'


@app.post('/predict')
def predict(video_id):
    # snapshot = tracemalloc.take_snapshot()
    # top_stats = snapshot.statistics('lineno')
    # print("Top 10")
    # for stat in top_stats[:10]:
    #     print(stat)
    print("Getting comments.")
    comments = get_comments(video_id)
    emotions = ["anger", "anticipation", "disgust", "fear", "joy",
                "love", "optimism", "hopeless", "sadness", "surprise", "trust"]
    comments = {
        comment_id: {
            'text': comments[comment_id]['text'],
            'emotions': {emotion: None for emotion in emotions}
        } for comment_id in comments
    }
    data = [comments[comment_id]['text'] for comment_id in comments]
    data_loader = preprocess(data, preprocessor)
    print("Making predictions.")
    with torch.no_grad():
        for i, batch in enumerate(data_loader):
            if i == 0:
                preds = list(model(batch, device)[1])
            else:
                preds += list(model(batch, device)[1])
    
    for comment_id, pred in zip(comments, preds):
        for emotion in emotions:
            comment = comments[comment_id]
            comment['emotions'][emotion] = int(pred[emotions.index(emotion)])
    
    stats = sum(preds)
    for emotion, stat in zip(emotions, stats):
        print(f"{emotion}: {int(stat)}")

    return {'Response': comments}
