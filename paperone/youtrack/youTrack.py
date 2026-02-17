import os
import requests
from datetime import datetime,timezone
import asyncio

YOUTRACK_TOKEN = os.getenv('YOUTRACK_TOKEN')
YOUTRACK_URL = os.getenv('YOUTRACK_URL')

youtrack_server_reachable = False
update_frequency = 24 #h
fields = 'id,idReadable,created,customFields(name,value(name))'
base_query= 'project; Kalliope Type: Bug'


def update_query(last_update):
    return f"{base_query} updated: {last_update} .. Now" #to check the type of the timestamp accepted by youtrack


def get_issues(fields,query):

    top = 1000
    skip = 0

    refetch = True

    issues = []

    while refetch:

        response = requests.get(
            headers={
                "Content-Type":"application/json",
                "Accept":"application/json",
                "Authorization":f"Bearer {YOUTRACK_TOKEN}"
            },
            params={
                "fields":fields,
                "query":query,
                "$top":top,
                "$skip":skip
            },
            url= f"{YOUTRACK_URL}/api/issues"
        )

        issue_data = response.json()

        refetch = False if len(issue_data)<top else True #if youtrack returns less items than requested it means he don-t have any more items to return
        
        skip += top
        newdata = []
        for issue in issue_data:
            issue['created'] = datetime.fromtimestamp(issue['created']/1000,tz=timezone.utc)
            issue['updated'] = datetime.fromtimestamp(issue['updated']/1000,tz=timezone.utc)

            newdata.append(issue)

        issues.extend(newdata)
    return issues


    async def youTrack_worker():

        if youtrack_server_reachable:
            issues = get_issues(fields=fields,query=base_query)
        else:
            with open('./issue.json') as p:
                issues = json.load(p)

        IssueRepository.bulk_create_issue_with_fields(issues_data=issues)

        last_pull = datetime.datetime.now()

        if youtrack_server_reachable:

            while True:
                await asyncio.sleep(
                    60*60*(update_frequency+1)
                )
                try:
                    issues = get_issues(fields=fields,query=update_query(last_pull))

                    last_pull = datetime.datetime.now()

                    IssueRepository.update_issues(issues)
                except Exception as e:
                    print(f"Error during sync with yt:{e}")
                    continue