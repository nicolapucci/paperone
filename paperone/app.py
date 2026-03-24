from fastapi import FastAPI
from services.issue_repository import IssueRepository
import uvicorn
import subprocess
from models.base import Base
from services.postgres_engine import engine
from services.test_repository import TestRepository
from services.product_repository import ProductRepository

app = FastAPI()


Base.metadata.create_all(engine)

@app.on_event("startup")
async def startup_event():
    subprocess.Popen(["python", "-m", "youtrack.youTrack"])
    TestRepository.upsert_tests(TestRepository.prepare_csv_for_import('./bugia_csv'))

@app.get('/okr1')
def OKR1():
    return IssueRepository.okr1()

@app.get('/okr2')
def OKR2():
    return IssueRepository.okr2()

@app.get('/test-over-fte')
def fte():
    return TestRepository.test_over_fte()

@app.get('/okr4')
def OKR4():
    return IssueRepository.okr4()




if __name__ == '__main__':
    uvicorn.run(app,host='0.0.0.0',port=8000)