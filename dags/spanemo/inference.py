from pathlib import Path

from dags.spanemo.data_loader import DataClass

def choose_model():
    return Path('dags/spanemo/models/model.pt')


def preprocess(comments):
    args = {
        '--lang': 'English',
        '--max-length': '128',
    }
    dataset = DataClass(args, comments=comments)
    batch = [dataset.inputs, dataset.lengths, dataset.label_indices]
    return batch
