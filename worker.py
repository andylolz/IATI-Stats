import os

import redis
from rq import Worker, Queue, Connection
import sentry_sdk
from sentry_sdk.integrations.rq import RqIntegration
from dotenv import load_dotenv


load_dotenv()

listen = ['high', 'default', 'low']

redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')

conn = redis.from_url(redis_url)

if __name__ == '__main__':
    SENTRY_DSN = os.getenv('SENTRY_DSN')
    if SENTRY_DSN:
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            integrations=[RqIntegration()],
        )

    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work(logging_level='WARN')
