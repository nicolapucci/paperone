from services.postgres_engine import engine
from models.issues import Base
from youtrack.youTrack import get_issues
from services.issue_repository import IssueRepository
from services.test_repository import TestRepository
import json

try:

    TestRepository.import_tests_from_csv('./export_bugia.csv')
    

except Exception as e:
    print(f'Unable to seed the db: {e}')