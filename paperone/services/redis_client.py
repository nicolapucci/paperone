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




def get_prova_data():

    data = redis_client.get("prova_data") or []
    data = json.loads(data)
    result = []
    for item in data:
        transformed_item = {
            k: (
                datetime.datetime.fromtimestamp(float(v))
                if v and k in ["first_assigned_to_TCoE", "last_set_as_done", "latest_completion","first_start"]
                else datetime.timedelta(milliseconds=float(v))
                if v and k in ["time_spent", "total_duration","idle_in_TCoE"]
                else v
            )
            for k, v in item.items()
        }
        result.append(transformed_item)
    return result


def set_prova_data(prova_data:list):

    json_serializable_data = [
        {
            k: (
                v.total_seconds()*1000 if isinstance(v, datetime.timedelta)
                else v.timestamp() if isinstance(v, datetime.datetime)
                else v
            )
            for k, v in item.items()
        }
        for item in prova_data
    ]

    redis_client.set("prova_data", json.dumps(json_serializable_data))