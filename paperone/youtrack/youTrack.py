from services.redis_client import (
    set_last_issue_pull,
    get_last_issue_pull,
    set_last_activities_pull,
    get_last_avtivities_pull
)
from services.issue_repository import IssueRepository
from services.logger import logger

from concurrent.futures import ThreadPoolExecutor

import aiohttp
import json
import asyncio
from datetime import datetime, timedelta
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

#We create a function to generate PNG snapshots of the Grafana dashboards,
#so that they can be used in the report without having to wait for the whole pull process to complete.
#The function fetches the dashboard templates from the "dashboards" folder,
#then for each template it fetches the PNG for each panel and saves it in the "snapshots" folder with a name that includes the dashboard name, panel name, timestamp and panel id (to avoid issues with panels with the same name).
async def generate_dashboard_pngs():
    #Load dashboard templates that grafana uses to generate the dashboards,
    #these templates are in JSON format and contain the information about the panels and the endpoint to fetch the PNG for each panel,
    #if the endpoint is not specified we will try to generate it using the name of the dashboard (after some formatting)
    dasboard_templates_dir = "dashboards"
    dashboard_templates = []
    for file in os.listdir(dasboard_templates_dir):
        if file.endswith(".json"):
            with open(os.path.join(dasboard_templates_dir, file), 'r') as f:
                dashboard_templates.append(json.load(f))
    if 'grafana-token.txt' in os.listdir("/app/shared/"):
        with open("/app/shared/grafana-token.txt", "r") as f:
            grafana_token = f.read().strip()
        base_url = "http://grafana:3000/render/d-solo"
        now = datetime.now().strftime('%Y-%m-%d_%H-%M')
        async with aiohttp.ClientSession() as session:
            for template in dashboard_templates:
                uid = template['uid']
                name = template['title']
                try:
                    endpoint = template['templating']['list'][0]['query']['infinityQuery']['url']
                except Exception as e:
                    logger.warning(f"Error fetching endpoint for dashboard {template['title']}: {e}")
                    endpoint = name.lower().replace(" ", "-") #if the template doesn't have an endpoint use the name of the dashboard as endpoint (after some formatting)   

                url = f"{base_url}/{uid}{endpoint}"

                panels = template['panels']

                logger.debug(f"About to fetch PNGs for dashboard @ {url}")
                for panel in panels:
                    panel_id = panel['id']
                    panel_grid = panel['gridPos']
                    panel_title = panel['title']
                    panel_title = panel_title.replace(" ", "_").replace("/","_") #avoid issues with file names
                    panel_title = panel_title if panel_title else f"panel_{panel_id}" #if the panel doesn't have a title use its id as title

                    logger.debug(f"Fetching PNG for panel {panel_id} of dashboard {name} with size {panel_grid['w']}x{panel_grid['h']}")

                    try:
                        async with session.get(
                            headers={
                                "Authorization":f"Bearer {grafana_token}"
                            },
                            params={
                                "width": panel_grid['w']*100,#We increase the size of the panel to have better quality in the PNG, but this also means that the generation of the PNG will take more time and resources, so we need to find a balance between quality and performance, for now we set it to 100x the original size, but this can be adjusted if needed
                                "height": panel_grid['h']*100,#same as width
                                "panelId": panel_id,
                                #We might want to add some parameters to the URL to filter the data in the panels,
                                #for example to only show data from the last month,
                                #this can be done by adding a time range parameter to the URL,
                                #for now we will just fetch the PNGs without any filters reliying on the default settings of the panels,
                                #but this can be easily implemented if needed
                            },
                            url=url
                        ) as response:
                            response.raise_for_status()
                            png_data = await response.read()

                            response.raise_for_status()

                            os.makedirs(f"snapshots/{name}", exist_ok=True)
                            
                            with open(f"snapshots/{name}/{panel_title}_{now}_{panel_id}.png", 'wb') as f:
                                f.write(png_data)
                    except Exception as e:
                        logger.warning(f"Error fetching PNG for panel {panel_id} of dashboard {name}: {e}")
                        continue
    else:
        logger.warning("Grafana token not found, skipping PNG generation")

def update_query(last_update):

    return f"{base_query} updated: {last_update} .. Now"


def upsert_issues_thread(chunk):
    IssueRepository.upsert_issues(issue_data=chunk)

#We fetch the issues from YouTrack in chunks of 3000 items,
#and for each chunk we create a new task in the ThreadPoolExecutor to insert the items in the database,
#so that we can fetch data from YouTrack asynchronously without having to wait for the database operations to complete.
async def get_issues(fields,query):

    top = 3000
    skip = 0

    refetch = True


    failed_calls_count = 0

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

                failed_calls_count = 0
                yield issue_data

        except Exception as e:
            refetch = failed_calls_count < 3
            failed_calls_count +=1
            logger.warning(f"Error fetching data: {e}")


async def process_issues(executor,query):
    tasks = []
    loop = asyncio.get_event_loop()

    async for chunk in get_issues(fields=fields,query=query):
        tasks.append(loop.run_in_executor(executor,upsert_issues_thread,chunk.copy()))
    
    await asyncio.gather(*tasks)

    return


def upsert_activity_items_thread(chunk):
    IssueRepository.upsert_activity_items(activity_item_data=chunk)

async def get_issue_activities(issue_id, fields,session):
    try:
        async with session.get(
            headers={
                "Content-Type":"application/json",
                "Accept":"application/json",
                "Authorization":f"Bearer {YOUTRACK_TOKEN}"
            },
            params = {
                "categories":activity_item_category,
                "fields":fields
            },
            url = f"{YOUTRACK_URL}/api/issues/{issue_id}/activities"
        ) as response:
            response.raise_for_status()
            issue_data = await response.json()
            return issue_data
    except Exception as e:
        logger.warning(f"Error fetching ActivityItem {issue_id}")
        raise e
       
#We fetch the activities for each issue asynchronously, but we still want to limit the number of concurrent requests to YouTrack to avoid overwhelming it, so we use a semaphore to only allow 4 concurrent requests at a time.
#We also use a ThreadPoolExecutor to run the blocking database operations in separate threads, so that they don't block the main event loop and we can still fetch data from YouTrack asynchronously.
# Finally, we use a buffer to store the fetched ActivityItems and insert them in batches of 3000 to improve performance.
async def process_issue_activities( executor,last_activities_pull, batch_size=3000):
    #we fetch the ids of the issues that have had activities since the last pull, then we fetch the activities for each issue asynchronously, and we insert them in the database in batches of <batch_size> to improve performance
    issue_ids = IssueRepository.get_validation_ids(last_activities_pull)
    #we use a semaphore to limit the number of concurrent requests to YouTrack, to avoid overwhelming it
    semaphore = asyncio.Semaphore(4)
    loop = asyncio.get_running_loop()

    buffer = []

    async def fetch(session, issue_id):
        async with semaphore:
            return await get_issue_activities(issue_id, activity_item_field, session)

    async with aiohttp.ClientSession() as session:

        tasks = [asyncio.create_task(fetch(session, i)) for i in issue_ids]

        for task in asyncio.as_completed(tasks):
            result = await task
            buffer.extend(result)

            # appena ho abbastanza dati → inserisco
            if len(buffer) >= batch_size:
                await loop.run_in_executor(
                    executor,
                    upsert_activity_items_thread,
                    buffer.copy()
                )
                buffer.clear()

        # flush finale
        if buffer:
            await loop.run_in_executor(
                executor,
                upsert_activity_items_thread,
                buffer.copy()
            )

async def youTrack_worker():
    while True:
        logger.info("About to pull data from YouTrack...")

        last_issue_pull = get_last_issue_pull()

        last_activities_pull = get_last_avtivities_pull()
        
        query=update_query((last_issue_pull- timedelta(hours=1)).strftime('%Y-%m')) if last_issue_pull else base_query#if there is a previous update only ask for data from 1h before that

        try:    
            with ThreadPoolExecutor(max_workers=5) as executor:#we use a thread pool executor to run the blocking database operations in separate threads, so that they don't block the main event loop and we can still fetch data from YouTrack asynchronously
                await process_issues(executor,query)
            set_last_issue_pull()
            logger.info("Issue pulling is completed.")
        except Exception as e:
            logger.error(f"Error doing issue pull {e}")

        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                await process_issue_activities(executor,last_activities_pull)
            set_last_activities_pull()
            logger.info("ActivityItem pulling is completed.")
        except Exception as e:
            logger.error(f"Error during ativities pull {e}")
        
        try:
            IssueRepository.okr2() #after pulling data from YouTrack we update the OKR2 data in Redis, so that the API can return up-to-date data without having to wait for the whole pull process to complete
        except Exception as e:
            logger.error(f"Error during OKR2 update: {e}")

        try:        
            IssueRepository.okr4() #same for OKR4
        except Exception as e:
            logger.error(f"Error during OKR4 update: {e}")

        try:
            await generate_dashboard_pngs()#after pulling data from YouTrack we generate new PNG snapshots of the Grafana dashboards, so that they can be used in the report without having to wait for the whole pull process to complete
        except Exception as e:
            logger.error(f"Error during PNG generation: {e}")

        await asyncio.sleep(
                    60*60*(update_frequency)
                )
    
if __name__ == '__main__':
    asyncio.run(youTrack_worker())
