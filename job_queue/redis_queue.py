import json
import redis
from scheduler.models import Job, JobStatus
import time


# Allows worker to comunicate with redis 
class RedisQueue:
    STREAM_NAME = "jobs"
    # define consumer group
    CONSUMER_GROUP = "workers"
    # jobs that are retrying
    RETRY_SET = "retry_jobs"

    def __init__(self, host: str = "localhost", port: int = 6379) :
        # get the redis and where redis is being hosted on.
        # default is local host
        self.client = redis.Redis(
            host=host,
            port=port,
            decode_responses=True, )

    # ping redis, it is a health check
    def ping(self) -> bool:
        return self.client.ping()

    # XAdd (redis append operation) Job into stream. Value is job id, type, payload, priority
    # xadd returns message_id
    # job id is different than redis key in message_id
    def enqueue(self, job: Job) -> str:
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

    # mark job a part of the retry set
    # ZADD adds members to a sorted set
    def schedule_retry(self, job: Job):
        self.client.zadd(
            self.RETRY_SET,
            # track retry timestamp, use as the score in the set
            {job.id: job.retry_at.timestamp()}
        )

    # ZRANGEBYSCORE retrieves members of a sorted set whose scores fall within a given range.
    # Get all retries in this case -inf to now
    def get_ready_retries(self):
        now = time.time()
        return self.client.zrangebyscore(
            self.RETRY_SET,
            "-inf",
            now,
        )
    
    # remove from retry set
    def remove_retry(self, job_id: str):
        self.client.zrem(
            self.RETRY_SET,
            job_id,
        )
    
    # redis streams are an append only-log. XREAD says give me messages from this stream. 
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

    # save a job in redis
    def save_job(self, job: Job) :
        self.client.set(f"job:{job.id}", job.model_dump_json(),) # stores pydantic model as json

    # get a job from redis
    def get_job(self, job_id:str) -> Job | None:
        # Redis returns raw bytes or a string, not a job object
        data = self.client.get(f"job:{job_id}")
        if data is None:
            return None
        # takes JSON data (the model JSON retrieved), parses it, validates it against Job model.
        # returns a JOB
        return Job.model_validate_json(data) 

    def create_consumer_group(self):
        try:
            # create a consumer group "workers", assocated with the stream "jobs",
            # create if does not exist  
            self.client.xgroup_create(
                self.STREAM_NAME, 
                self.CONSUMER_GROUP,
                # $ - means start from new messages
                id="$",
                mkstream=True
            )
        # we are calling this everytime a worker starts. The first worker creates "workers"
        # second worker tries to create it and it says the name alr exsits. Not really an error.
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    # acknowledge a Job message has been processed
    # important to call so a Job is not processed again unecessarily
    def ack(self, message_id: str) -> int :
        return self.client.xack(self.STREAM_NAME, self.CONSUMER_GROUP, message_id)

    # Worker can call this to claim any pending jobs
    # XAUTOCLAIM
    def claim_pending(self, consumer_name: str, min_idle_ms: int = 60000,
                        count: int = 10) : 
        result = self.client.xautoclaim(
            self.STREAM_NAME,
            self.CONSUMER_GROUP,
            consumer_name,
            min_idle_ms,
            # cursor argument, tells redis where in the PEL (Pending Entries List) to start
            # getting claimable messages
            # 0-0 is the lowest possible id, start from beginning
            "0-0",
            count=count,
        )
        if not result : 
            return []
        return result[1]
    
    # create a hashset of worker records
    def register_worker(self, worker_id: str) : 
        key = f"worker:{worker_id}"
        self.client.hset(key, mapping=
                                {"worker_id" : worker_id, 
                                "status" : "idle",
                                "current_job" : "",
                                "last_heartbeat" : time.time(),
                                "jobs_processed": 0})
    
    # updates the heart_beat of worker to show it is alive
    # using worker id as the key
    def heartbeat_worker(self, worker_id: str):
        key = f"worker:{worker_id}"
        self.client.hset(
            key,
            mapping={
                "last_heartbeat": time.time(),
            })
    
    # worker should tell redis when it starts processing a job
    # updates current job, status, and last_heartbeat to now
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

    # get all workers
    def get_workers(self) -> list[str]:
        keys = self.client.keys("worker:*")
        return [
            key.split(":", 1)[1]
            for key in keys
        ]

    # check if worker is alive. Using the heartbeat and a timeout parameter.
    def is_worker_alive(self, worker_id: str, timeout_seconds: int = 15,) -> bool: 
        key = f"worker:{worker_id}"
        last_heartbeat = self.client.hget(key, "last_heartbeat")
        if last_heartbeat is None:
            return False
        elapsed = time.time() - float(last_heartbeat)
        return elapsed < timeout_seconds

    # unused now.
    # meant to claim one dead workers jobs for a new worker
    def claim_worker_jobs(self, dead_worker_id: str, new_worker_id: str,
                           min_idle_ms: int = 15_000, count: int = 10) : 
        pending = self.client.xpending_range(
            name=self.STREAM_NAME,
            groupname=self.CONSUMER_GROUP,
            min="-",
            max="+",
            count=count,
            consumername=dead_worker_id,
            idle=min_idle_ms,
        )

        if not pending :
            return []

        # get all of the ids we need to claim from the dead worker
        ids_to_claim = [entry["message_id"] for entry in pending]

        # claim just those IDs for the new worker
        claimed = self.client.xclaim(
            name=self.STREAM_NAME,
            groupname=self.CONSUMER_GROUP,
            consumername=new_worker_id,
            min_idle_time=min_idle_ms,
            message_ids=ids_to_claim,
        )
        return claimed  

    # get all pending jobs
    def get_pending_jobs(self, count: int = 100):
        result = self.client.xpending_range(
            self.STREAM_NAME,
            self.CONSUMER_GROUP,
            min="-",
            max="+",
            count=count)
        return result

    # get jobs that have a running status
    def get_running_jobs(self) -> list[Job]:
        jobs = []
        keys = self.client.keys("job:*")
        for key in keys:
            data = self.client.get(key)
            if data is None:
                continue
            job = Job.model_validate_json(data)
            if job.status == JobStatus.RUNNING:
                jobs.append(job)
        return jobs