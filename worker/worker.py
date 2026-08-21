from job_queue.redis_queue import RedisQueue
import json
from .executer import execute_job

from datetime import datetime, timezone, timedelta
from scheduler.models import Job, JobStatus
import threading
import time
from scheduler.retry_manager import RetryManager


# worker continuously asks redis, is there work? if so process job or no wait.
class Worker:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.queue = RedisQueue()
        self.queue.create_consumer_group()
        self.queue.register_worker(worker_id)
        heartbeat_thread = threading.Thread(
            target=self.heartbeat_loop,
            daemon=True)
        heartbeat_thread.start()
        self.retry_manager = RetryManager(self.queue)

    def run(self):
        print(f"Worker {self.worker_id} started")
        while True:
            messages = self.queue.consume(consumer_name=self.worker_id)
            if not messages:
                continue
            for stream_name, entries in messages:
                for message_id, data in entries:
                    self.process_job(message_id, data)

    def process_job(self, message_id: str, data: dict):
        print(data)
        job_id = data["job_id"]
        job = self.queue.get_job(job_id)
        if job is None:
            print(f"[{self.worker_id}] Job {job_id} not found")
            return

        # job is marked as running, set status and start time and this worker
        job.status = JobStatus.RUNNING
        job.started_at = datetime.now(timezone.utc)
        job.worker_id = self.worker_id
        job.message_id = message_id
        self.queue.save_job(job)

        print( f"[{self.worker_id}] " f"Processing job {job.id}")

        try:
            # Execute job
            # Execute the job
            result = execute_job(
                job.type,
                job.payload,
            )
            current_job = self.queue.get_job(job.id)
            if current_job is None:
                return
            if current_job.status != JobStatus.RUNNING:
                print(
                    f"[{self.worker_id}] "
                    f"Job {job.id} is no longer running. "
                    f"Ignoring result."
                )
                return

            # Mark job as COMPLETED
            current_job.status = JobStatus.COMPLETED
            current_job.completed_at = datetime.now(timezone.utc)
            current_job.result = result
            self.queue.save_job(current_job)
            self.queue.ack(message_id)

            print(
                f"[{self.worker_id}] "
                f"Completed job {job.id}: {result}"
            )
        except Exception as e:
            self.retry_manager.schedule_retry(
                job,
                message_id,
                e,
            )

    def recover_jobs(self) :
        messages = self.queue.claim_pending(
            consumer_name=self.worker_id,
            min_idle_ms=100_000,
            count=10,
        )
        for message_id, data in messages:
            print(
                f"[{self.worker_id}] "
                f"Recovered job {data['job_id']}"
            )
            self.process_job(message_id, data)

    def heartbeat_loop(self) :
        while True :
            self.queue.heartbeat_worker(self.worker_id)
            time.sleep(5)