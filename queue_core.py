# queue_core.py
import json
import os
import time
import uuid
import subprocess
from pathlib import Path
from contextlib import contextmanager

BASE_DIR = Path(__file__).parent
DATA_FILE = BASE_DIR / "queue_data.json"
CONFIG_FILE = BASE_DIR / "config.json"
WORKERS_FILE = BASE_DIR / "workers.json"
LOCK_FILE = BASE_DIR / "queue_data.lock"


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@contextmanager
def file_lock(timeout=5):
    """
    Simple atomic lock using lock file creation.
    Retries until timeout (seconds).
    """
    start = time.time()
    while True:
        try:
            # use os.O_CREAT | os.O_EXCL so creation fails if exists (atomic)
            fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            break
        except FileExistsError:
            if time.time() - start > timeout:
                raise TimeoutError("Could not acquire lock")
            time.sleep(0.05)
    try:
        yield
    finally:
        try:
            os.remove(str(LOCK_FILE))
        except FileNotFoundError:
            pass


def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    default = {"max_retries": 3, "backoff_base": 2}
    with open(CONFIG_FILE, "w") as f:
        json.dump(default, f, indent=2)
    return default


def save_workers(pids):
    with open(WORKERS_FILE, "w") as f:
        json.dump({"pids": pids}, f)


def load_workers():
    if WORKERS_FILE.exists():
        with open(WORKERS_FILE, "r") as f:
            return json.load(f).get("pids", [])
    return []


class Job:
    def __init__(self, *, id=None, command=None, state="pending", attempts=0, max_retries=None, created_at=None, updated_at=None):
        self.id = id or uuid.uuid4().hex
        self.command = command or ""
        self.state = state  # pending, processing, completed, failed, dead
        self.attempts = attempts
        self.max_retries = max_retries  # allow override per job; if None use config
        self.created_at = created_at or now_iso()
        self.updated_at = updated_at or now_iso()

    def to_dict(self):
        return {
            "id": self.id,
            "command": self.command,
            "state": self.state,
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @staticmethod
    def from_dict(d):
        return Job(
            id=d.get("id"),
            command=d.get("command"),
            state=d.get("state", "pending"),
            attempts=d.get("attempts", 0),
            max_retries=d.get("max_retries"),
            created_at=d.get("created_at"),
            updated_at=d.get("updated_at"),
        )


class JobStore:
    def __init__(self):
        self.data_file = DATA_FILE
        if not self.data_file.exists():
            self._init_file()
        self.config = load_config()

    def _init_file(self):
        empty = {"jobs": [], "dlq": []}
        with open(self.data_file, "w") as f:
            json.dump(empty, f, indent=2)

    def _read(self):
        with open(self.data_file, "r") as f:
            return json.load(f)

    def _write(self, obj):
        with open(self.data_file, "w") as f:
            json.dump(obj, f, indent=2)

    def enqueue(self, job: Job):
        with file_lock():
            data = self._read()
            data["jobs"].append(job.to_dict())
            self._write(data)

    def list_jobs(self, state=None):
        with file_lock():
            data = self._read()
            jobs = [Job.from_dict(j) for j in data.get("jobs", [])]
        if state:
            return [j for j in jobs if j.state == state]
        return jobs

    def list_dlq(self):
        with file_lock():
            data = self._read()
            return [Job.from_dict(j) for j in data.get("dlq", [])]

    def claim_job(self, worker_id=None):
        """
        Atomically find a job with state 'pending', mark it 'processing' and return it.
        """
        with file_lock():
            data = self._read()
            for idx, j in enumerate(data.get("jobs", [])):
                if j.get("state") == "pending":
                    job = Job.from_dict(j)
                    job.state = "processing"
                    job.updated_at = now_iso()
                    # write back
                    data["jobs"][idx] = job.to_dict()
                    self._write(data)
                    return job
            return None

    def update_job_after_run(self, job: Job, success: bool, move_to_dlq=False):
        """
        If success: mark completed and remove from jobs list -> append to completed (we'll just remove).
        If fail/retry: either requeue (pending) or move to dlq (dead).
        """
        with file_lock():
            data = self._read()
            # find job in jobs by id
            found_idx = None
            for idx, j in enumerate(data.get("jobs", [])):
                if j.get("id") == job.id:
                    found_idx = idx
                    break

            if success:
                # remove from jobs list
                if found_idx is not None:
                    data["jobs"].pop(found_idx)
                # no separate completed list required by spec; state will be stored in record if wanted
            else:
                if move_to_dlq:
                    # remove from jobs list and add to dlq
                    if found_idx is not None:
                        data["jobs"].pop(found_idx)
                    job.state = "dead"
                    job.updated_at = now_iso()
                    data.setdefault("dlq", []).append(job.to_dict())
                else:
                    # put back as pending
                    if found_idx is not None:
                        data["jobs"][found_idx] = job.to_dict()
                    else:
                        data.setdefault("jobs", []).append(job.to_dict())

            self._write(data)

    def remove_dlq_job(self, job_id):
        with file_lock():
            data = self._read()
            new_dlq = [j for j in data.get("dlq", []) if j.get("id") != job_id]
            data["dlq"] = new_dlq
            self._write(data)

    def requeue_dlq_job(self, job_id):
        with file_lock():
            data = self._read()
            match = None
            for idx, j in enumerate(data.get("dlq", [])):
                if j.get("id") == job_id:
                    match = j
                    data["dlq"].pop(idx)
                    break
            if match:
                match["state"] = "pending"
                match["updated_at"] = now_iso()
                data.setdefault("jobs", []).append(match)
                self._write(data)
                return True
            return False

    def counts(self):
        with file_lock():
            data = self._read()
            jobs = data.get("jobs", [])
            dlq = data.get("dlq", [])
            pending = sum(1 for j in jobs if j.get("state") == "pending")
            processing = sum(1 for j in jobs if j.get("state") == "processing")
            completed = 0  # we don't keep a separate completed list in this simple design
            failed = len(dlq)
            return {"pending": pending, "processing": processing, "failed": failed, "total_jobs": len(jobs)}

    def get_job(self, job_id):
        with file_lock():
            data = self._read()
            for j in data.get("jobs", []) + data.get("dlq", []):
                if j.get("id") == job_id:
                    return Job.from_dict(j)
        return None
