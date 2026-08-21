from job_queue.redis_queue import RedisQueue
from worker.worker_monitor import WorkerMonitor

queue = RedisQueue()
monitor = WorkerMonitor(queue)

monitor.run()