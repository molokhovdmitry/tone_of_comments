from torch.utils.data import DataLoader

from spanemo.data_loader import DataClass


def preprocess(comments, preprocessor, tokenizer):
    """Loads `comments` list into `DataLoader` class."""
    args = {
        '--lang': 'English',
        '--max-length': '128',
    }
    dataset = DataClass(args,
                        comments=comments,
                        preprocessor=preprocessor,
                        tokenizer=tokenizer)
    data_loader = DataLoader(dataset, batch_size=256, shuffle=False)
    return data_loader
