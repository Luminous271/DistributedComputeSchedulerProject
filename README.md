# IDEA

Distributed Compute Scheduler

A fault-tolerant distributed compute platform for scheduling heterogeneous workloads across dynamically scaled workers.

# INTRODUCTION
This is a distributed systems project I made to learn Dsitributed systems. It is something I used to strengthen my docker and SWE skills.

# MOTIVATION

Modern cloud platforms execute large numbers of computational jobs across distributed worker nodes. This project implements a simplified batch-computing system to explore scheduling, fault tolerance, workload distribution, and horizontal scaling.

# ARCHITECTURE


# What is a Job?
- Version 0.1.0
Job
├── id - id number
├── type - what type of computation should be performed
├── payload - input to the computation
├── priority - scheduling priority
├── status - current state of lifecycle
├── created_at - when it was submitted
├── started_at - when execution began
├── completed_at - when execution finished
├── worker_id - what worker is current executing it
├── retry_count - number of execution attempts
└── result - output of computation
- We want a unit of computation submitted to the final distributed compute system.
- Life-Cycle:
- QUEUED -> RUNNING -> COMPLETED or (FAILED -> RETRYING -> QUEUED) or CANCELD
 