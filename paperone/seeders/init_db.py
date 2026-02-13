from services.postgres_engine import engine
from models.issues import Base
from youtrack.youTrack import get_issues
from services.issue_repository import IssueRepository

Base.metadata.create_all()

fields = 'id,idReadable,created,customFields(name,value(name))'
base_query= 'project; Kalliope Type: Bug'

try:
    issues = get_issues(fields=fields,query=base_query)

    IssueRepository.bulk_create_issue_with_fields(issues_data=issues)
except Exception as e:
    print(f'Unable to seed the db: {e}')