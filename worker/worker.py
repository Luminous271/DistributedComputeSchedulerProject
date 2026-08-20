from job_queue.redis_queue import RedisQueue
import json
from .executer import execute_job

# worker continuously asks redis, is there work? if so process job or no wait.
class Worker:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.queue = RedisQueue()

    def run(self):
        print(f"Worker {self.worker_id} started")

        while True:
            messages = self.queue.consume()
            if not messages:
                continue
            for stream_name, entries in messages:
                for message_id, data in entries:
                    self.process_job(message_id, data)

    def process_job(self, message_id: str, data: dict):
        print(
            f"[{self.worker_id}] "
            f"Processing job {data['job_id']}"
        )
        result = execute_job(data["type"], json.loads(data["payload"]))

        print(
            f"[{self.worker_id}] "
            f"Completed job {data['job_id']}: {result}"
        )