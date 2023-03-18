from pathlib import Path

from torch.utils.data import DataLoader

from dags.spanemo.data_loader import DataClass

def choose_model():
    return Path('dags/spanemo/models/model.pt')


def preprocess(comments, preprocessor):
    args = {
        '--lang': 'English',
        '--max-length': '128',
    }
    dataset = DataClass(args, comments=comments, preprocessor=preprocessor)
    data_loader = DataLoader(dataset, batch_size=256, shuffle=False)
    return data_loader
