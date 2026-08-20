from job_queue.redis_queue import RedisQueue
import json
from .executer import execute_job

from datetime import datetime, timezone
from scheduler.models import JobStatus

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
        job_id = data["job_id"]
        job = self.queue.get_job(job_id)
        if job is None:
            print(f"[{self.worker_id}] Job {job_id} not found")
            return

        # job is marked as running, set status and start time and this worker
        job.status = JobStatus.RUNNING
        job.started_at = datetime.now(timezone.utc)
        job.worker_id = self.worker_id
        self.queue.save_job(job)

        print( f"[{self.worker_id}] " f"Processing job {job.id}")

        try:
            # Execute job
            # Execute the job
            result = execute_job(
                job.type,
                job.payload,
            )

            # Mark job as COMPLETED
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now(timezone.utc)
            job.result = result

            self.queue.save_job(job)

            print(
                f"[{self.worker_id}] "
                f"Completed job {job.id}: {result}"
            )
        except Exception as e:
            # Mark job as FAILED
            job.status = JobStatus.FAILED
            job.completed_at = datetime.now(timezone.utc)
            job.result = {"error": str(e)}

            self.queue.save_job(job)

            print(
                f"[{self.worker_id}] "
                f"Failed job {job.id}: {e}"
            )