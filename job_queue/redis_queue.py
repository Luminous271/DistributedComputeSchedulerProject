import json
import redis
from scheduler.models import Job


class RedisQueue:
    STREAM_NAME = "jobs"
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.client = redis.Redis(
            host=host,
            port=port,
            decode_responses=True,
        )

    def ping(self) -> bool:
        return self.client.ping()

    def enqueue(self, job: Job) -> str:
        # XAdd (redis append operation) Job into stream. Value is job id, type, payload, priority
        # xadd returns message_id
        # job id is different than redis key in message_id
        message_id = self.client.xadd(
            self.STREAM_NAME,
            {
                "job_id": job.id,
                "type": job.type,
                "payload": json.dumps(job.payload),
                "priority": job.priority,
            },
        )
        return message_id