import redis
import os
import datetime
import json
      
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT',6379)


redis_client = redis.Redis(host=REDIS_HOST,port=REDIS_PORT,decode_responses=True)


def get_youtrack_last_sync():
    return redis_client.get('youtrack_sync_timestamp')

def set_youtrack_last_sync():
    now = datetime.datetime.now().strftime('%Y-%m')
    redis_client.set('youtrack_sync_timestamp',now)




def get_changelog_releases():
    releases = redis_client.hgetall('changelog_releases')
    return {k: datetime.datetime.fromtimestamp(float(v)) for k, v in releases.items()}

def set_changelog_releases(releases:dict):
    redis_client.delete('changelog_releases')
    for release_key, release_value in releases.items():
        redis_client.hset('changelog_releases', release_key, release_value.timestamp())




def get_okr2_data():
    raw_data = redis_client.get('okr2')
    data = json.loads(raw_data)
    for item in data:
        for k,v in item.items():
            if isinstance(v,dict):
                item[k] = datetime.datetime.fromtimestamp(float(v["value"])) if v["type"] == "datetime" else datetime.timedelta(seconds=float(v["value"]))
    return data

def set_okr2_data(data:list):
    new_list = []
    for item in data:
        new_obj = {}
        for k,v in item.items():
            if isinstance(v,datetime.datetime):
                new_obj[k] = {"value":v.timestamp(),"type":"datetime"}
            elif isinstance(v,datetime.timedelta):
                new_obj[k] = {"value":v.total_seconds(),"type":"timedelta"}
            else:
                new_obj[k] = v
        new_list.append(new_obj)
    redis_client.set('okr2',json.dumps(new_list))




def get_okr4_data():
    raw_data = redis_client.get('okr4')
    if not raw_data:
        return None

    data = json.loads(raw_data)
    for item in data:
        for k,v in item.items():
            if isinstance(v,dict):
                item[k] = datetime.datetime.fromtimestamp(float(v["value"])) if v["type"] == "datetime" else datetime.timedelta(seconds=float(v["value"]))
    return data

def set_okr4_data(data:list):
    new_list = []
    for item in data:
        new_obj = {}
        for k,v in item.items():
            if isinstance(v,datetime.datetime):
                new_obj[k] = {"value":v.timestamp(),"type":"datetime"}
            elif isinstance(v,datetime.timedelta):
                new_obj[k] = {"value":v.total_seconds(),"type":"timedelta"}
            else:
                new_obj[k] = v
        new_list.append(new_obj)
    redis_client.set('okr4',json.dumps(new_list))