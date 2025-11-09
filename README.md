# QueueCTL - Background Job Queue System

## Tech Stack
**Language:** Python 3.12  
**Storage:** JSON File Persistence  
**Libraries Used:** `argparse`, `subprocess`, `multiprocessing`, `json`, `os`, `time`, `uuid`

---

## Objective
Build a CLI-based background job queue system called **`queuectl`**.

This system manages background jobs with worker processes, automatically retries failed jobs using **exponential backoff**, and maintains a **Dead Letter Queue (DLQ)** for permanently failed jobs.

The design focuses on reliability, persistence, and easy CLI-based control.

---

## Problem Overview
This project implements a minimal, production-grade job queue system that supports:
- Enqueuing and managing background jobs  
- Running multiple worker processes concurrently  
- Retrying failed jobs automatically  
- Exponential backoff for retry delays  
- Moving permanently failed jobs to a **Dead Letter Queue (DLQ)**  
- Persistent job storage using JSON across restarts  
- CLI interface for all operations  

---

## Job Specification
Each job includes the following fields:

```json
{
  "id": "unique-job-id",
  "command": "echo 'Hello World'",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2025-11-04T10:30:00Z",
  "updated_at": "2025-11-04T10:30:00Z"
}
```
---
## Setup Instructions
Prerequisites
- Python 3.10 or higher
- Git installed on your system

Steps:
1. Clone the repository
 ```
   git clone https://github.com/manasamurali63/queuectl.git
   cd queuectl
```
2. Run commands using Python:
```
python queue_ctl.py --help
```
No external libraries are required since the implementation only uses Python’s standard library.
## Usage Examples
- Enqueue a Job
  ```
  python queue_ctl.py enqueue --command "echo hello && exit 0"
  ```
- Start Multiple Workers
  ```
  python queue_ctlStarted worker pid: 8728
  ```
- Check Status
  ```
  python queue_ctl.py status
  ```
- View Dead Letter Queue
  ```
  python queue_ctl.py dlq list
  ```
- Retry a Job from DLQ
  ```
  python queue_ctl.py dlq retry fe6ad23dd7274bd6a699ac08fcadde87
  ```
- Stop All Workers
  ```
  python queue_ctl.py worker stop
  ```
  ---
## Architecture Overview
### Job Lifecycle
- **pending:** Waiting to be processed  
- **processing:** Currently executing  
- **completed:** Successfully executed  
- **failed:** Failed but will retry  
- **dead:** Permanently failed, moved to DLQ  

### Data Persistence
- Jobs are stored in a JSON file (`jobs.json`) in the project directory.  
- Each change in job state is saved immediately to ensure persistence across restarts.  
- The Dead Letter Queue (DLQ) is also persisted in the same JSON file for recovery.  

### Worker Logic
- Workers are created as separate processes using the Python `multiprocessing` module.  
- Each worker continuously polls the queue for available jobs.  
- The worker executes the job command using the `subprocess` module.  
- Success or failure is determined by the process exit code.  
- Failed jobs are retried using exponential backoff (`delay = base ^ attempts`).  
- When the retry limit is reached, the job is moved to the Dead Letter Queue.  
- Workers can be started or stopped gracefully from the CLI.  

## Assumptions & Trade-offs

### Storage
- JSON file is used for persistence instead of a database to keep the project lightweight.

### Retries
- Default maximum retries are 3.  
- Exponential backoff base value is 2 seconds.

### Worker Management
- Multiple workers can run in parallel, but all run locally on the same system.

### Simplifications
- Priority queues, scheduled jobs, and web dashboards are not implemented.  
- Configuration and state files are stored locally.  
- File-level locking is used for concurrency control.  

## Testing Instructions 

### Test 1: Successful Job
```bash
python queue_ctl.py enqueue --command "echo test && exit 0"
python queue_ctl.py worker start --count 1
```
Expected Result: Job completes successfully and shows as "completed".
### Test 2: Failed Job with Retries
```
python queue_ctl.py enqueue --command "exit 1"
python queue_ctl.py worker start --count 1
```
Expected Result: Job fails, retries 3 times with increasing delays (1s, 2s, 4s), then moves to DLQ.

### Test 3: Persistence
- Enqueue jobs and start workers.
- Stop the script or close the terminal.
- Re-run:
  ```
  python queue_ctl.py status
  ```
Expected Result: Job states are still available after restart.

### Test 4: Retry from DLQ
```
python queue_ctl.py dlq list
python queue_ctl.py dlq retry <jobid>
```
Expected Result: The job is re-enqueued and retried again.
### Test 5: Worker Stop
```
python queue_ctl.py worker stop
```
Expected Result: Workers finish their current job before shutting down.

## Demo Video
Watch the CLI demo here: [Demo Video](https://drive.google.com/file/d/1ivzozeJ4u6Eq8rHzrHQLeY9AkzMBBLWi/view?usp=drive_link)
