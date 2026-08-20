import json
import redis
from scheduler.models import Job


class RedisQueue:
    STREAM_NAME = "jobs"
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.client = redis.Redis(
            host=host,
            port=port,
            decode_responses=True, )

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

    # redis streams are an append only-log. Xread says give me messages from this stream. 
    # The block is If there isn't a job right now, wait up to 5 seconds 
    # rather than constantly hammering Redis for messages. Blocking consumption.
    def consume(self, count: int = 1, block_ms: int = 5000):

        messages = self.client.xread(
            {self.STREAM_NAME: "0-0"},
            count=count,
            block=block_ms,
        )
        return messages

    def save_job(self, job: Job) :
        self.client.set(f"job:{job.id}", job.model_dump_json(),) # stores pydantic model as json

    def get_job(self, job_id:str) -> Job | None:
        data = self.client.get(f"job:{job_id}")
        if data is None:
            return None
        # takes JSON data (the model JSON retrieved), parses it, validates it against Job model.
        return Job.model_validate_json(data) 