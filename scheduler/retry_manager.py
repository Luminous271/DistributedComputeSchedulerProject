from scheduler.models import JobStatus, Job
from datetime import datetime, timezone, timedelta
from job_queue.redis_queue import RedisQueue    
import time

# class that handels all worker retrys 
class RetryManager:
    def __init__(self, queue: RedisQueue):
        self.queue = queue

    # any jobs that are actively retrying
    def process_ready_retries(self):
        job_ids = self.queue.get_ready_retries()
        for job_id in job_ids:
            # load job
            job = self.queue.get_job(job_id)
            # remove from retry set
            if job is None:
                self.queue.remove_retry(job_id)
                continue
            # set status QUEUED
            job.status = JobStatus.QUEUED
            job.retry_at = None
            # enqueue job
            self.queue.save_job(job)
            self.queue.remove_retry(job_id)
            self.queue.enqueue(job)
            print(
                f"[RetryManager] "
                f"Requeued job {job.id}"
            )

    # schedule a retry, update retry count
    def schedule_retry(self, job: Job, message_id: str, e: Exception) -> bool:
        job.retry_count += 1
        job.result = {"error": str(e)}

        if job.retry_count > job.max_retries:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.now(timezone.utc)
            job.result = {"error": str(e)}

            self.queue.save_job(job)
            self.queue.ack(message_id)

            print(f"[{job.worker_id}] "
                    f"Failed job {job.id}: {e}")
            return False

        delay = 2 ** job.retry_count

        job.status = JobStatus.RETRYING
        job.retry_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=delay)
        )

        # save updated job
        self.queue.save_job(job)
         # Put the job into Redis' delayed retry set
        self.queue.schedule_retry(job)

        # We're done with the original stream message
        self.queue.ack(message_id)

        print(
            f"[{job.worker_id}] "
            f"Retrying job {job.id} "
            f"({job.retry_count}/{job.max_retries})"
        )

        # Don't enqueue yet.
        # We'll put it into the delayed-retry structure here.
        return True

    # deprecated function, timeouts are handeled by the execution controller
    def schedule_timeout(self, job: Job, error: Exception) -> bool:
        current_job = self.queue.get_job(job.id)

        if current_job is None:
            return False

        if current_job.status != JobStatus.RUNNING:
            print(
                f"[RetryManager] "
                f"Job {job.id} is no longer running"
            )
            return False

        current_job.status = JobStatus.TIMED_OUT
        current_job.completed_at = datetime.now(timezone.utc)
        current_job.result = {
            "error": str(error)
        }

        self.queue.save_job(current_job)

        print(
            f"[RetryManager] "
            f"Timed out job {current_job.id}: {error}"
        )

        return True

    # run the retry manager
    def run(self):
        while True:
            self.process_ready_retries()
            time.sleep(1)