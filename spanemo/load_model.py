import torch
import wandb

from ekphrasis.classes.tokenizer import SocialTokenizer
from ekphrasis.classes.preprocessor import TextPreProcessor
from transformers import BertTokenizer

from spanemo.model import SpanEmo


# Choose the device.
device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
if str(device) == 'cuda:0':
    print("Currently using GPU: {}".format(device))
else:
    print("Currently using CPU")

# Load the model from wandb with `best` alias.
api = wandb.Api()
artifact_name = "molokhovdmitry/tone_of_comments/emotions_model:"
artifact = api.artifact(artifact_name + "best", type='model')
model_version = artifact.version
print(f"Model version: {model_version}")
artifact_dir = artifact.file("spanemo/artifacts")
model = SpanEmo()
model.load_state_dict(torch.load(artifact_dir))
model.eval()
model.to(device)

# Load the preprocessor.
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

# Load the tokenizer.
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased', do_lower_case=True)
