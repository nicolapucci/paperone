from services.postgres_engine import engine
from models.issues import Base
from youtrack.youTrack import get_issues
from services.issue_repository import IssueRepository
from services.test_repository import TestRepository
import json

Base.metadata.create_all(engine)

fields = 'id,idReadable,created,customFields(name,value(name))'
base_query= 'project; Kalliope Type: Bug'

try:
    #issues = get_issues(fields=fields,query=base_query)
    
    #with open('./issue.json') as p:
        #issues = json.load(p)

        #IssueRepository.bulk_create_issue_with_fields(issues_data=issues)

    TestRepository.import_tests_from_csv('./export_bugia.csv')
    

except Exception as e:
    print(f'Unable to seed the db: {e}')