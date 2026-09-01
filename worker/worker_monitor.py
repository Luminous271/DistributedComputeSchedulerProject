import time
from job_queue.redis_queue import RedisQueue

class WorkerMonitor:
    def __init__(self, queue: RedisQueue, check_interval: int = 5, 
                        heartbeat_timeout: int = 15):
        self.queue = queue
        self.check_interval = check_interval
        self.heartbeat_timeout = heartbeat_timeout

    # check workers for any deaths
    def check_workers(self):
        # get workers
        workers = self.queue.get_workers()

        # for all workers
        for worker_id in workers:
            # check if alive
            alive = self.queue.is_worker_alive(
                worker_id,
                self.heartbeat_timeout,
            )
            # healthy or death
            if alive:
                print(
                    f"[Monitor] "
                    f"{worker_id} is healthy"
                )
            else:
                print(
                    f"[Monitor] "
                    f"{worker_id} is DEAD"
                )
                # get replacement worker (any healthy worker)
                replacement = self.get_healthy_worker( exclude=worker_id )
                # recover those jobs
                if replacement:
                    self.recover_dead_worker(
                        dead_worker_id=worker_id,
                        replacement_worker_id=replacement,
                    )
    # run check workers after time check interval
    def run(self):
            while True:
                self.check_workers()
                time.sleep(self.check_interval)

    def recover_dead_worker(self, dead_worker_id: str, replacement_worker_id: str):
        # get all pending jobs
        pending = self.queue.get_pending_jobs()

        for entry in pending:
            # check if entry is from deadworker
            if entry["consumer"] != dead_worker_id:
                continue

            message_id = entry["message_id"]

            print(
                f"[Monitor] Recovering "
                f"{message_id} from {dead_worker_id}"
            )
            # claim that dead job
            messages = self.queue.claim_pending(
                consumer_name=replacement_worker_id,
                min_idle_ms=15_000,
                count=1,
            )
            # print statements for all reclaimed jobs
            for message_id, data in messages:
                print(
                    f"[Monitor] "
                    f"{replacement_worker_id} claimed "
                    f"job {data['job_id']}"
                )
    # get all health workers (are they alive)
    def get_healthy_worker(self, exclude: str):
        workers = self.queue.get_workers()

        for worker_id in workers:
            if worker_id == exclude:
                continue

            if self.queue.is_worker_alive(worker_id):
                return worker_id

        return None