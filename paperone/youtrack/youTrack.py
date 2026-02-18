import os
import requests
from datetime import datetime,timezone
import asyncio

from services.issue_repository import IssueRepository

from services.redis_client import (
    set_youtrack_last_sync,
    get_youtrack_last_sync
)
import json

from services.logger import logger


YOUTRACK_TOKEN = os.getenv('YOUTRACK_TOKEN')
YOUTRACK_URL = os.getenv('YOUTRACK_URL')

youtrack_server_reachable = True
update_frequency = 24 #h

fields = 'id,idReadable,summary,created,updated,customFields(name,value(name))'
base_query= 'project: Kalliope Type: Bug'


def update_query(last_update):
    return f"{base_query} updated: {last_update} .. Now" #to check the type of the timestamp accepted by youtrack


def get_issues(fields,query):

    top = 1000
    skip = 0

    refetch = True

    issues = []

    while refetch:
        try:
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
            response.raise_for_status()

            issue_data = response.json()

            refetch = False if len(issue_data)<top else True #if youtrack returns less items than requested it means he don-t have any more items to return
            
            skip += top
            newdata = []
            for issue in issue_data:
                newdata.append(issue)

            issues.extend(newdata)
        except Exception as e:
            logger.warning(f"Error fetching data: {e}")
            refetch = False
    return issues


async def youTrack_worker():

    while True:
        logger.info("Starting Issue sync...")

        last_sync = get_youtrack_last_sync()

        last_sync = IssueRepository.get_max_updated_issue() if not last_sync else last_sync
            
        if youtrack_server_reachable:
            logger.info("Youtrack Server is reachable, fetching data...")
            issues = get_issues(fields=fields,query=update_query(last_sync) if last_sync else base_query)

        
        else:
            logger.info("Youtrack Server is unreachable, loading data from local file...")
            try:
                with open('./issue.json') as p:
                    issues = json.load(p)
            except Exception as e:
                logger.error(f"Error loading local file.")
                issues = []
        if issues:
            logger.info(f"{len(issues)} retrieved, saving Issue data...")
            IssueRepository.upsert_issues(issue_data=issues)
            set_youtrack_last_sync()
        else:
            logger.warning("No issues to save.")

        await asyncio.sleep(
                    60*60*(update_frequency+1)
                )
    
if __name__ == '__main__':
    asyncio.run(youTrack_worker())