from fastapi import FastAPI
from services.issue_repository import IssueRepository
import uvicorn
import threading
from models.base import Base
from youtrack.youTrack import youTrack_worker
from services.postgres_engine import engine


app = FastAPI()


Base.metadata.create_all(engine)

thread = threading.Thread(target=youTrack_worker)
thread.start()

@app.get('/defect-rate')
def defect_rate():
    return IssueRepository.defect_rate()

if __name__ == '__main__':
    uvicorn.run(app,host='0.0.0.0',port='8000')