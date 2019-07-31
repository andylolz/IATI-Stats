import os

import redis
from rq import Worker, Queue, Connection
from rq.contrib.sentry import register_sentry


listen = ['high', 'default', 'low']

redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')

conn = redis.from_url(redis_url)

if os.getenv('SENTRY_DSN'):
    register_sentry(os.getenv('SENTRY_DSN'))

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work()
