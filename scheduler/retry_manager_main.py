from job_queue.redis_queue import RedisQueue
from scheduler.retry_manager import RetryManager


queue = RedisQueue()
retry_manager = RetryManager(queue)

retry_manager.run()