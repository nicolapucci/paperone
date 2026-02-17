from fastapi import FastAPI
from services.issue_repository import IssueRepository
import uvicorn
import threading
from models.Base import Base

app = FastAPI()


Base.metadata.create_all(engine)


@app.get('/defect-rate')
def defect_rate():
    return IssueRepository.defect_rate()

if __name__ == '__main__':
    uvicorn.run(app,host='0.0.0.0',port='8000')