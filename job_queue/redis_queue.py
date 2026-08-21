import json
import redis
from scheduler.models import Job
import time

class RedisQueue:
    STREAM_NAME = "jobs"
    CONSUMER_GROUP = "workers"
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

    def retry(self, job: Job, message_id) : 
        self.save_job(job)
        self.enqueue(job)
        self.ack(message_id)

    # redis streams are an append only-log. Xread says give me messages from this stream. 
    # The block is If there isn't a job right now, wait up to 5 seconds 
    # rather than constantly hammering Redis for messages. Blocking consumption.
    # Added read from a specific group. 
    # Give me new messages that have never been delivered to another consumer in this group. 
    def consume(self, consumer_name: str, count: int = 1, block_ms: int = 5000):
        messages = self.client.xreadgroup(
            groupname=self.CONSUMER_GROUP,
            consumername=consumer_name,
            streams={self.STREAM_NAME: ">"},
            count=count,
            block=block_ms )
        return messages

    def save_job(self, job: Job) :
        self.client.set(f"job:{job.id}", job.model_dump_json(),) # stores pydantic model as json

    def get_job(self, job_id:str) -> Job | None:

        data = self.client.get(f"job:{job_id}")
        if data is None:
            return None
        # takes JSON data (the model JSON retrieved), parses it, validates it against Job model.
        return Job.model_validate_json(data) 

    def create_consumer_group(self):
        try:
            # create a consumer group "workers", assocated with the stream "jobs",
            # $ - means start from new messages
            # create if does not exist  
            self.client.xgroup_create(
                self.STREAM_NAME, 
                self.CONSUMER_GROUP,
                id="$",
                mkstream=True
            )
        # we are calling this everytime a worker starts. The first worker creates "workers"
        # second worker tries to create it and it says the name alr exsits. Not really an error.
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def ack(self, message_id: str) -> int :
        return self.client.xack(self.STREAM_NAME, self.CONSUMER_GROUP, message_id)

    def claim_pending(self,
                        consumer_name: str,
                        min_idle_ms: int = 60000,
                        count: int = 10) : 
        return self.client.xautoclaim(
            self.STREAM_NAME,
            self.CONSUMER_GROUP,
            consumer_name,
            min_idle_ms,
            "0-0",
            count=count,
        )
        return result[1]
    # create a hashset of worker records
    def register_worker(self, worker_id: str) : 
        key = f"worker:{worker_id}"
        self.client.hset(key, mapping=
                                {"worker_id" : worker_id, 
                                "status" : "idle",
                                "current_job" : "",
                                "last_heartbeat" : time.time(),
                                "jobs_processed": 0}
                        )
    # updates the heart_beat of worker to show it is alive
    def heartbeat_worker(self, worker_id: str):
        key = f"worker:{worker_id}"
        self.client.hset(
            key,
            mapping={
                "last_heartbeat": time.time(),
            })
    # worker should tell redis when it starts processing a job
    def set_worker_status(self, worker_id: str, status: str, current_job: str | None = None) :
        key = f"worker:{worker_id}"
        self.client.hset(key, mapping = {
                "status": status,
                "current_job": current_job or "",
                "last_heartbeat": time.time() } )

    # method to get the worker record
    def get_worker(self, worker_id: str):
        key = f"worker:{worker_id}"
        return self.client.hgetall(key)

    def get_workers(self) -> list[str]:
        keys = self.client.keys("worker:*")
        return [
            key.split(":", 1)[1]
            for key in keys
        ]

    def is_worker_alive(self, worker_id: str, timeout_seconds: int = 15,) -> bool: 
        key = f"worker:{worker_id}"
        last_heartbeat = self.client.hget(key, "last_heartbeat")
        if last_heartbeat is None:
            return False
        elapsed = time.time() - float(last_heartbeat)
        return elapsed < timeout_seconds

    def claim_worker_jobs(self, dead_worker_id: str, new_worker_id: str,
                           min_idle_ms: int = 15_000, count: int = 10) : 
        result = self.client.xautoclaim(self.STREAM_NAME, self.CONSUMER_GROUP, 
                                            new_worker_id, 
                                            min_idle_ms, 
                                            "0-0",
                                            count=count)
        return result[1]
    
    def get_pending_jobs(self, count: int = 100):
        result = self.client.xpending_range(
            self.STREAM_NAME,
            self.CONSUMER_GROUP,
            min="-",
            max="+",
            count=count )
        return result