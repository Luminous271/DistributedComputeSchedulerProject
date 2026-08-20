import time 

def execute_job(job_type: str, payload: dict) :
    if job_type == "sleep" :

        duration_ms = payload["duration_ms"]

        # time sleep
        time.sleep(duration_ms / 1000)

        # return message in json
        return { "message": f"Slept for {duration_ms} ms" }

    # if job type not supported
    raise ValueError(f"Job type not supported by ecectuer: {job_type}")
