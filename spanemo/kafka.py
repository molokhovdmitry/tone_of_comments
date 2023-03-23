import json

import torch
from ekphrasis.classes.tokenizer import SocialTokenizer
from ekphrasis.classes.preprocessor import TextPreProcessor

from spanemo.model import SpanEmo
from spanemo.inference import choose_model, preprocess
from kafka.consumer import consume_loop, find_message
from kafka.producer import produce_to_topic

def load_model():
    """Loads the model and preprocessor."""

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


def predict(msg):
    try:
        # Unpack message.
        video_id = msg.key().decode()
        comments = json.loads(msg.value())

        # Create a dictionary with emotions.
        emotions = ["anger", "anticipation", "disgust", "fear", "joy",
                    "love", "optimism", "hopeless", "sadness", "surprise", "trust"]
        comments = {
            comment_id: {
                'text': comments[comment_id]['text'],
                'emotions': {emotion: None for emotion in emotions}
            } for comment_id in comments
        }
    except Exception as e:
        print(f"Key: {msg.key()}", end='\n')
        print(f"Value: {msg.value()}", end='\n')
        print(e)
        return

    # Don't predict if predictions were already made.
    preds = find_message(video_id, ['emotions'], keep_trying=False)
    if preds is not None:
        print(f"Skipped {video_id}.")
        return
    print(f"Predicting for {video_id}.")

    # Get texts from `comments` dictionary and make predictions.
    data = [comments[comment_id]['text'] for comment_id in comments]
    data_loader = preprocess(data, preprocessor)
    print("Making predictions.")
    with torch.no_grad():
        for i, batch in enumerate(data_loader):
            if i == 0:
                preds = list(model(batch, device)[1])
            else:
                preds += list(model(batch, device)[1])

    # Fill `comments` with emotions.
    for comment_id, pred in zip(comments, preds):
        for emotion in emotions:
            comment = comments[comment_id]
            comment['emotions'][emotion] = int(pred[emotions.index(emotion)])

    # Produce `comments` to `emotions` topic.
    produce_to_topic('emotions', video_id, comments)


if __name__ == '__main__':
    # Load the model.
    load_model()
    print("Model loaded.")

    # Start the consumer that is subscribed to `comments` topic.
    consume_loop(['comments'], predict)
