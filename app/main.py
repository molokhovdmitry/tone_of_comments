from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.generator import generate_preds

app = FastAPI(title='tone_of_comments')


app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get('/')
def home():
    return 'tone_of_comments'


@app.post("/predict")
async def predict(video_id):
    """Streams comments with predictions."""
    return StreamingResponse(generate_preds(video_id), media_type="application/json")
