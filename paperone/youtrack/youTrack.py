from services.redis_client import (
    set_youtrack_last_sync,
    get_youtrack_last_sync
)
from services.issue_repository import IssueRepository
from services.logger import logger

from concurrent.futures import ThreadPoolExecutor

from datetime import datetime,timezone

import aiohttp
import asyncio
import json
import os



"""
    youTrack_worker perform 1 cicle every <update_frequency> hours,
    in each cicle he fetches Issue and Ativityitems data from YouTrack (if he has info abt a previous update he only takes data since 1h before last update),
    then uses IssueRepository to save the items
"""


YOUTRACK_TOKEN = os.getenv('YOUTRACK_TOKEN') 
YOUTRACK_URL = os.getenv('YOUTRACK_URL')

#Time (hours) to wait before re-pulling data from YouTrack
update_frequency = 1

#YouTrack will return issues matching the following query
base_query= 'project: Kalliope, TCoE'

#YouTrack requires us to declare the name of the fields we want him to return in the API requests
fields = 'id,idReadable,summary,created,updated,customFields(name,value(name,text,fullName,minutes)),parent(issues(idReadable)),links(issues(idReadable),linkType(name),direction),tags(name)'

#same as field but for ActivityItems
activity_item_field = 'id,author(id,login,name),timestamp,added(id,idReadable,name,value(name,text,fullName,minutes)),removed(id,idReadable,name,value(name,text,fullName,minutes)),target(id,idReadable),targetMember'

#We need to specify the category of ActivityItems we want YouTrack to return(CustomFieldCategory will return Issue custom Fields)
activity_item_category = 'CustomFieldCategory'

def update_query(last_update):
    return f"{base_query} updated: {last_update} .. Now"



def upsert_issues_thread(chunk):
    IssueRepository.upsert_issues(issue_data=chunk)

async def get_issues(fields,query):#sync fetch issues from YouTrack

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
                        "fields":fields,
                        "query":query,
                        "$top":top,
                        "$skip":skip
                    },
                    url= f"{YOUTRACK_URL}/api/issues"
                ) as response:

                    response.raise_for_status()

                    issue_data = await response.json()

                    refetch = False if len(issue_data)<top else True #if youtrack returns less items than requested it means he don-t have any more items to return
                    
                    skip += top

                yield issue_data

        except Exception as e:
            logger.warning(f"Error fetching data: {e}")
            refetch = False

async def process_issues(executor,query):
    tasks = []
    loop = asyncio.get_event_loop()

    async for chunk in get_issues(fields=fields,query=query):
        tasks.append(loop.run_in_executor(executor,upsert_issues_thread,chunk.copy()))
    
    await asyncio.gather(*tasks)

    return


def upsert_activity_items_thread(chunk):
    IssueRepository.upsert_activity_items(activity_item_data=chunk)

async def get_activity_items(fields,query,categories):#async fetch ActivityItems from YouTrack

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
                        "issueQuery":"project: Kalliope summary: \"(Integration Test Verification)\"",#query,
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

async def process_activity_items(executor, query):#Uses ThreadPoolExecutor to concurrently upsert batches of ActivityItems received from get_activity_items()
    tasks = []
    loop = asyncio.get_event_loop()

    #get_activity_items yields abt 3k items, we gather them in batches of 6k to reduce the number of iterations needed,
    #we don't want to create big batches to avoid overloading a worker
    async for chunk in get_activity_items(fields=activity_item_field, categories=activity_item_category, query=query):
        #every chunk we create a task to upsert the data in the batch
        tasks.append(loop.run_in_executor(executor, upsert_activity_items_thread, chunk.copy()))
        
    #we launch the tasks
    await asyncio.gather(*tasks)

    return

async def youTrack_worker():
    logger.debug('worker start')
    while True:
        logger.info("Starting Issue sync...")

        last_sync = get_youtrack_last_sync()#timestamp from the last cicle saved in redis

        last_sync = IssueRepository.get_max_updated_issue() if not last_sync else last_sync#if there is nothing on redis then check the most recend issue.updated from the saved issues
            
        query=update_query(last_sync) if last_sync else base_query#if there is a previous update only ask for data from 1h before that

        try:            

            with ThreadPoolExecutor(max_workers=5) as executor:
                await process_issues(executor,query)

            with ThreadPoolExecutor(max_workers=5) as executor:
                await process_activity_items(executor,query)

            logger.info("YouTrack Sync successfully completed")

            set_youtrack_last_sync()#set last sync at now
        
        except Exception as e:
            logger.info(f"Error during YouTrack syncronization: {e}")
            raise
        await asyncio.sleep(
                    60*60*(update_frequency)
                )
    
if __name__ == '__main__':
    asyncio.run(youTrack_worker())
