from job_queue.redis_queue import RedisQueue

def test_worker_is_alive():
    queue = RedisQueue()

    queue.register_worker("worker-1")

    assert queue.is_worker_alive("worker-1") is True