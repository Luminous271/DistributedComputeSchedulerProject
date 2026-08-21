from job_queue.redis_queue import RedisQueue
from scheduler.worker_monitor import WorkerMonitor

queue = RedisQueue()
monitor = WorkerMonitor(queue)

monitor.run()