# IDEA

Distributed Compute Scheduler

A fault-tolerant distributed compute platform for scheduling heterogeneous workloads across dynamically scaled workers.

# INTRODUCTION
This is a distributed systems project I made to learn Dsitributed systems. It is something I used to strengthen my docker and SWE skills.

# MOTIVATION

Modern cloud platforms execute large numbers of computational jobs across distributed worker nodes. This project implements a simplified batch-computing system to explore scheduling, fault tolerance, workload distribution, and horizontal scaling.

# ARCHITECTURE

### System Overview

```text
                         ┌──────────────────┐
                         │      Client      │
                         │  REST / Web UI   │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │     FastAPI      │
                         │   API Server     │
                         └────────┬─────────┘
                                  │
                         Submit / Query Jobs
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │          Redis          │
                    │       Job Queue         │
                    │      Redis Streams      │
                    └────────────┬────────────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │ Worker 1 │ │ Worker 2 │ │ Worker N │
              └────┬─────┘ └────┬─────┘ └────┬─────┘
                   │             │             │
                   └─────────────┼─────────────┘
                                 ▼
                         ┌────────────────┐
                         │ Job Execution  │
                         │    Engine      │
                         └────────────────┘
```
### Job Model

The scheduler separates job submission from the internal representation of
a job.

#### `JobRequest`

`JobRequest` represents the data provided by a client when submitting a new
job. It contains only the information necessary to create the job:

- `type` — type of computation to perform
- `payload` — parameters required by the computation
- `priority` — scheduling priority from 0–10

Fields such as job ID, timestamps, worker assignment, and execution results
are intentionally excluded because these values are generated or managed by
the scheduler. This is what the client can use to request jobs.

#### `Job`

`Job` represents the complete lifecycle of a job within the scheduler.

In addition to the request data, it contains system-managed information such
as:

- `id` — unique identifier assigned by the scheduler
- `status` — current execution state
- `created_at` — time the job was submitted
- `started_at` — time execution began
- `completed_at` — time execution finished
- `worker_id` — worker currently responsible for the job
- `retry_count` — number of execution attempts
- `result` — output produced by the job

The `Job` model therefore acts as the scheduler's internal representation
of a job and provides the information needed to track its execution.

#### `JobStatus`

`JobStatus` is an enumeration defining the valid states in a job's lifecycle:

```text
QUEUED → RUNNING → COMPLETED
            │
            ▼
          FAILED
            │
            ▼
         RETRYING
            │
            ▼
          QUEUED