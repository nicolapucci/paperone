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

fields = 'id,idReadable,summary,created,updated,customFields(name,value(name)),parent(issues(idReadable))'
base_query= 'project: Kalliope Type: Bug'

activity_item_field = 'id,author(id,login,name),timestamp,added(id,idReadable,name,value),removed(id,idReadable),target(id,idReadable),targetMember'
activity_item_category = 'CustomFieldCategory'

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


def get_activity_items(fields,query,categories):

    top = 1000
    skip = 0

    refetch = True

    activity_items = []

    while refetch:
        try:
            response = requests.get(
                headers={
                    "Content-Type":"application/json",
                    "Accept":"application/json",
                    "Authorization":f"Bearer {YOUTRACK_TOKEN}"
                },
                params={
                    "categories":categories,
                    "fields":fields,
                    "issueQuery":query,
                    "$top":top,
                    "$skip":skip,
                },
                url= f"{YOUTRACK_URL}/api/activities"
            )
            response.raise_for_status()

            activity_item_data = response.json()
            logger.debug(f"received{activity_item_data}")

            refetch = False if len(activity_item_data)<top else True #if youtrack returns less items than requested it means he don-t have any more items to return
            
            skip += top
            newdata = []
            for activity_item in activity_item_data:
                newdata.append(activity_item)

            activity_items.extend(newdata)
        except Exception as e:
            logger.warning(f"Error fetching data: {e}")
            refetch = False
    return activity_items


async def youTrack_worker():

    while True:
        logger.info("Starting Issue sync...")

        last_sync = get_youtrack_last_sync()

        last_sync = IssueRepository.get_max_updated_issue() if not last_sync else last_sync
            
        if youtrack_server_reachable:

            query=update_query(last_sync) if last_sync else base_query

            logger.info("Youtrack Server is reachable, fetching data...")
            issues = get_issues(
                fields=fields,
                query=query
                )

            activity_items = get_activity_items(
                fields=activity_item_field,
                categories=activity_item_category,
                query=query
                )
        
        else:
            logger.info("Youtrack Server is unreachable, loading data from local file...")
            try:
                with open('./issue.json') as p:
                    issues = json.load(p)
            except Exception as e:
                logger.error(f"Error loading local file.")
                issues = []
        try:

            if issues:
                IssueRepository.upsert_issues(issue_data=issues)
            if activity_items:
                IssueRepository.upsert_activity_items(activity_item_data=activity_items)

            upserted_issues = len(issues) if issues else 0
            upserted_activity_items = len(activity_items) if activity_items else 0
            log_string = f"Added {upserted_issues} Issues and {upserted_activity_items} activity Items" if issues or activity_items else "Nothing to add"

            logger.info(log_string)

            set_youtrack_last_sync()
        
        except Exception as e:
            logger.info(f"Error during YouTrack syncronization: {e}")
            raise
        await asyncio.sleep(
                    60*60*(update_frequency+1)
                )
    
if __name__ == '__main__':
    asyncio.run(youTrack_worker())