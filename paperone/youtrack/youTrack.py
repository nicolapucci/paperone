import os
import requests
import aiohttp
from datetime import datetime,timezone
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

from services.issue_repository import IssueRepository

from services.redis_client import (
    set_youtrack_last_sync,
    get_youtrack_last_sync
)
import json

from services.logger import logger


YOUTRACK_TOKEN = os.getenv('YOUTRACK_TOKEN') 
YOUTRACK_URL = os.getenv('YOUTRACK_URL')

issue_json_path = './issue.json'

youtrack_server_reachable = True
update_frequency = 24 #h

fields = 'id,idReadable,summary,created,updated,customFields(name,value(name,text,fullName,minutes)),parent(issues(idReadable))'
base_query= 'project: Kalliope'
#base_query= 'summary: "(Integration Test Verification)"'
#base_query= 'project: Kalliope Type: Bug'
#base_query = 'project: Kalliope Type: Bug Assignee: TCoE Stage: Done State: Verified'
activity_item_field = 'id,author(id,login,name),timestamp,added(id,idReadable,name,value),removed(id,idReadable),target(id,idReadable),targetMember'
activity_item_category = 'CustomFieldCategory'

def update_query(last_update):
    return f"{base_query} updated: {last_update} .. Now" #to check the type of the timestamp accepted by youtrack

def get_issues_from_json(filepath):
    with open(filepath) as p:
        return json.load(p)

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

def upsert_issues_thread(issues):
    IssueRepository.upsert_issues(issue_data=issues)

def upsert_activity_items_thread(chunk):
    IssueRepository.upsert_activity_items(activity_item_data=chunk)

async def get_activity_items(fields,query,categories):

    top = 3000
    skip = 0

    refetch = True

    while refetch:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
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
                ) as response:

                    response.raise_for_status()

                    activity_item_data = await response.json()

                    refetch = False if len(activity_item_data)<top else True #if youtrack returns less items than requested it means he don-t have any more items to return
                    
                    skip += top

                yield activity_item_data
        except Exception as e:
            logger.warning(f"Error fetching data: {e}")
            refetch = False

async def process_activity_items(executor, query):
    batch = []
    activity_items_count = 0
    tasks = []
    loop = asyncio.get_event_loop()

    async for chunk in get_activity_items(fields=activity_item_field, categories=activity_item_category, query=query):
        batch.extend(chunk)

        if len(batch) >= 6000:
            tasks.append(loop.run_in_executor(executor, upsert_activity_items_thread, batch.copy()))
            activity_items_count += len(batch)
            batch = []

    if batch:
        tasks.append(loop.run_in_executor(executor, upsert_activity_items_thread, batch))
        activity_items_count += len(batch)

    await asyncio.gather(*tasks)

    return activity_items_count


async def youTrack_worker():

    while True:
        logger.info("Starting Issue sync...")

        last_sync = get_youtrack_last_sync()

        last_sync = IssueRepository.get_max_updated_issue() if not last_sync else last_sync
            
        query=update_query(last_sync) if last_sync else base_query

        issues = get_issues(fields=fields,query=query) if youtrack_server_reachable else get_issues_from_json(issue_json_path)

        logger.info('abt to upsert')
        IssueRepository.upsert_issues(issues)
        try:            
            activity_items_count = 0

            # EVALUATE THE BEST WAY TO EXECUTE THE WORKER AS A SEPARATE PROCESS
            batch = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                activity_items_count += await process_activity_items(executor,query)

            upserted_issues = len(issues) if issues else 0
            log_string = f"Added {upserted_issues} Issues and {activity_items_count} activity Items" if issues or activity_items else "Nothing to add"

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