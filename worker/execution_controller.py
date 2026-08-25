from multiprocessing import Process, Queue
from typing import Any

from worker.executer import execute_job
GRACE_PERIOD_SECONDS = 0.5

def _run_job(job_type: str, payload: dict, result_queue: Queue) :
    try:   
        result = execute_job(job_type, payload)
        result_queue.put({"success": True, "result": result})
    except Exception as e:
        result_queue.put({
            "success": False,
            "error": str(e),
        })

def execute_with_timeout(job_type: str, payload: dict, timeout_seconds: int) -> Any:

    result_queue = Queue()
    # create a new process
    process = Process(
        target=_run_job,
        args=(job_type, payload, result_queue),
    )
    process.start()
    process.join(timeout_seconds) # join the processes after the timeout seconds
    if process.is_alive():
        process.join(GRACE_PERIOD_SECONDS)
    if process.is_alive():
        print(
            f"[ExecutionController] "
            f"Job exceeded {timeout_seconds}s. "
            f"Terminating process {process.pid}")

        process.terminate()
        process.join()

        raise TimeoutError(
            f"Job exceeded timeout of {timeout_seconds} seconds"
        )

    if result_queue.empty():
        raise RuntimeError(
            "Job process exited without returning a result"
        )

    result = result_queue.get()

    if not result["success"]:
        raise RuntimeError(result["error"])

    return result["result"]