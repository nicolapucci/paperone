import redis
import os
import datetime
      
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'),6379)


redis_client = redis.Redis(host=REDIS_HOST,port=REDIS_PORT,decode_responses=True)


def get_youtrack_last_sync():
    return redis_client.get('youtrack_sync_timestamp')

def set_youtrack_last_sync():
    redis_client.set('youtrack_sync_timestamp',datetime.datetime.now())