from job_queue.redis_queue import RedisQueue
from scheduler.models import Job, JobStatus
from datetime import datetime, timezone

def test_redis_connection():
    queue = RedisQueue()

    assert queue.ping() is True


def test_enqueue_job():
    queue = RedisQueue()

    job = Job(
        id="test-job-1",
        type="matrix_multiply",
        payload={"size": 100},
        priority=5,
        status=JobStatus.QUEUED,
        created_at=datetime.now(timezone.utc),
        )

    message_id = queue.enqueue(job)

    assert message_id is not None