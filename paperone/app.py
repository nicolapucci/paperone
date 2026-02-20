from fastapi import FastAPI
from services.issue_repository import IssueRepository
import uvicorn
import subprocess
from models.base import Base
from services.postgres_engine import engine
from services.test_repository import TestRepository


app = FastAPI()


Base.metadata.create_all(engine)

@app.on_event("startup")
async def startup_event():
    subprocess.Popen(["python", "-m", "youtrack.youTrack"])
    TestRepository.upsert_tests(TestRepository.prepare_csv_for_import('./bugia_csv'))

@app.get('/defect-rate')
def defect_rate():
    return IssueRepository.defect_rate()

@app.get('/test-over-fte')
def fte():
    return TestRepository.test_over_fte()

if __name__ == '__main__':
    uvicorn.run(app,host='0.0.0.0',port=8000)