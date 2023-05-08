"""
Usage:
    main.py [options]

Options:
    -h --help                         show this screen
    --model-path=<str>                path of the trained model
    --max-length=<int>                text length [default: 128]
    --seed=<int>                      seed [default: 0]
    --test-batch-size=<int>           batch size [default: 32]
    --lang=<str>                      language choice [default: English]
    --test-path=<str>                 file path of the test set [default: ]
"""
from pathlib import Path
import wandb
from learner import EvaluateOnTest
from model import SpanEmo
from data_loader import DataClass
from torch.utils.data import DataLoader
import torch
from docopt import docopt
import numpy as np

args = docopt(__doc__)
device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
if str(device) == 'cuda:0':
    print("Currently using GPU: {}".format(device))
    np.random.seed(int(args['--seed']))
    torch.cuda.manual_seed_all(int(args['--seed']))
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
else:
    print("Currently using CPU")
#####################################################################
# Define Dataloaders
#####################################################################
test_dataset = DataClass(args, args['--test-path'])
test_data_loader = DataLoader(test_dataset,
                              batch_size=int(args['--test-batch-size']),
                              shuffle=False)
print('The number of Test batches: ', len(test_data_loader))
#############################################################################
# Run the model on a Test set
#############################################################################
from_wandb = args['--model-path'] == 'wandb'

if from_wandb:
    # Load the latest model.
    api = wandb.Api()
    artifact_name = "molokhovdmitry/tone_of_comments/emotions_model:"
    artifact = api.artifact(artifact_name + "latest", type='model')
    model_path = artifact.file("artifacts")

    # Try to load best model's F1 score.
    try:
        best_artifact = api.artifact(artifact_name + "best", type='model')
        best_f1_score = best_artifact.metadata["f1_macro"]
    except:
        best_f1_score = 0
else:
    model_path = 'models/' + args['--model-path']
model = SpanEmo(lang=args['--lang'])
learn = EvaluateOnTest(model, test_data_loader, model_path=model_path)
learn.predict(device=device)

if from_wandb:
    # Update model's metadata.
    f1_macro = learn.f1_macro
    f1_micro = learn.f1_micro
    jaccard_score = learn.jaccard_score
    artifact.metadata["f1_macro"] = f1_macro
    artifact.metadata["f1_micro"] = f1_micro
    artifact.metadata["jaccard_score"] = jaccard_score

    # Give `best` alias to the model if it has higher F1 score.
    if learn.f1_macro > best_f1_score:
        artifact.aliases.append('best')
        print("Gave the model `best` alias.")
    artifact.save()
    print("Saved the model.")
