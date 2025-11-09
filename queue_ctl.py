# queue_ctl.py
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from queue_core import Job, JobStore, DATA_FILE, WORKERS_FILE, BASE_DIR, load_config, save_workers, load_workers

PY = sys.executable  


def enqueue_command(args):
    store = JobStore()
    # enqueue accepts either a raw JSON string or a simple command string
    if args.json:
        try:
            jobdict = json.loads(args.json)
            job = Job(command=jobdict.get("command") or jobdict.get("cmd"), max_retries=jobdict.get("max_retries"))
        except Exception as e:
            print("Invalid JSON for enqueue:", e)
            return
    else:
        job = Job(command=args.command, max_retries=args.max_retries)
    store.enqueue(job)
    print("Enqueued:", job.id)


def run_worker_loop(worker_id, backoff_base, default_max_retries):
    store = JobStore()
    print(f"[worker-{worker_id}] started (pid={os.getpid()})")
    stop_file = BASE_DIR / f"worker_{worker_id}.stop"
    try:
        while True:
            if stop_file.exists():
                print(f"[worker-{worker_id}] stop file detected; exiting after current job.")
                break
            job = store.claim_job(worker_id=worker_id)
            if not job:
                time.sleep(0.5)
                continue
            # determine job max_retries
            job_max = job.max_retries if job.max_retries is not None else default_max_retries
            print(f"[worker-{worker_id}] picked job {job.id} -> {job.command}")
            # run the command
            try:
                # run with shell so commands like sleep / echo work
                proc = subprocess.run(job.command, shell=True)
                success = (proc.returncode == 0)
            except Exception as e:
                print(f"[worker-{worker_id}] exception running command: {e}")
                success = False

            if success:
                job.state = "completed"
                job.updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                store.update_job_after_run(job, success=True)
                print(f"[worker-{worker_id}] job {job.id} completed.")
            else:
                job.attempts += 1
                job.updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                if job.attempts >= job_max:
                    # move to dlq
                    print(f"[worker-{worker_id}] job {job.id} failed permanently (attempts={job.attempts}). Moving to DLQ.")
                    store.update_job_after_run(job, success=False, move_to_dlq=True)
                else:
                    # backoff
                    delay = (backoff_base ** (job.attempts - 1))
                    print(f"[worker-{worker_id}] job {job.id} failed — retrying after {delay}s (attempt {job.attempts}/{job_max})")
                    # set state back to pending and update attempts/time
                    job.state = "pending"
                    store.update_job_after_run(job, success=False, move_to_dlq=False)
                    time.sleep(delay)
    except KeyboardInterrupt:
        print(f"[worker-{worker_id}] interrupted; exiting")
    print(f"[worker-{worker_id}] exiting.")


def worker_start(args):
    count = args.count or 1
    config = load_config()
    pids = []
    for i in range(count):
        # launch new python process running this script with subcommand 'worker-process'
        cmd = [PY, os.path.join(os.path.dirname(__file__), "queue_ctl.py"), "worker-process", "--id", str(int(time.time()*1000) % 100000 + i)]
        # detach / background
        if os.name == "nt":
            # windows
            proc = subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        else:
            proc = subprocess.Popen(cmd)
        pids.append(proc.pid)
        print("Started worker pid:", proc.pid)
    # save pids
    save_workers(pids)


def worker_stop(args):
    # read pids and terminate
    pids = load_workers()
    if not pids:
        print("No workers running.")
        return
    for pid in pids:
        try:
            print("Stopping pid", pid)
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(pid), "/F"])
            else:
                os.kill(pid, signal.SIGTERM)
        except Exception as e:
            print("Error stopping pid", pid, e)
    # remove workers file
    if os.path.exists(WORKERS_FILE):
        os.remove(WORKERS_FILE)
    print("Workers stopped.")


def worker_process_main(args):
    cfg = load_config()
    backoff_base = cfg.get("backoff_base", 2)
    default_max_retries = cfg.get("max_retries", 3)
    worker_id = args.id or str(os.getpid())
    run_worker_loop(worker_id, backoff_base, default_max_retries)


def cmd_status(args):
    store = JobStore()
    c = store.counts()
    pids = load_workers()
    print("\n--- STATUS ---")
    print(f"Pending: {c['pending']}")
    print(f"Processing: {c['processing']}")
    print(f"Failed (DLQ): {c['failed']}")
    print(f"Active workers (known): {len(pids)}")
    print("----------------\n")


def cmd_list(args):
    store = JobStore()
    state = args.state
    jobs = store.list_jobs(state=state)
    if not jobs:
        print("No jobs found.")
        return
    for j in jobs:
        print(json.dumps(j.to_dict(), indent=2))


def cmd_dlq_list(args):
    store = JobStore()
    jobs = store.list_dlq()
    if not jobs:
        print("DLQ empty.")
        return
    for j in jobs:
        print(json.dumps(j.to_dict(), indent=2))


def cmd_dlq_retry(args):
    store = JobStore()
    ok = store.requeue_dlq_job(args.job_id)
    if ok:
        print("Requeued DLQ job", args.job_id)
    else:
        print("Job not found in DLQ:", args.job_id)


def cmd_config(args):
    cfg = load_config()
    if args.action == "get":
        print(json.dumps(cfg, indent=2))
    elif args.action == "set":
        key = args.key
        val = args.value
        if val is None:
            print("Please provide value to set.")
            return
        try:
            if val.isdigit():
                val = int(val)
        except Exception:
            pass
        cfg[key] = val
        with open(os.path.join(os.path.dirname(__file__), "config.json"), "w") as f:
            json.dump(cfg, f, indent=2)
        print("Config updated.")


def main():
    parser = argparse.ArgumentParser(prog="queue_ctl")
    sub = parser.add_subparsers(dest="cmd")

    # enqueue
    p = sub.add_parser("enqueue")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--json", help="Job JSON string (e.g. '{\"command\":\"sleep 2\"}')")
    group.add_argument("--command", help="Simple command string to run (shell).")
    p.add_argument("--max-retries", type=int, help="Override max_retries for this job.")

    # worker management
    p = sub.add_parser("worker")
    wsub = p.add_subparsers(dest="action")
    wstart = wsub.add_parser("start")
    wstart.add_argument("--count", type=int, default=1)
    wstop = wsub.add_parser("stop")

    # internal worker-process (background)
    wp = sub.add_parser("worker-process")
    wp.add_argument("--id", help="worker id")

    # status
    sub.add_parser("status")

    # list
    p = sub.add_parser("list")
    p.add_argument("--state", choices=["pending", "processing", "completed", "failed"], help="filter by state")

    # dlq
    p = sub.add_parser("dlq")
    dsub = p.add_subparsers(dest="action")
    dsub.add_parser("list")
    dretry = dsub.add_parser("retry")
    dretry.add_argument("job_id")

    # config
    p = sub.add_parser("config")
    csub = p.add_subparsers(dest="action")
    cget = csub.add_parser("get")
    cset = csub.add_parser("set")
    cset.add_argument("key")
    cset.add_argument("value")

    args = parser.parse_args()

    if args.cmd == "enqueue":
        enqueue_command(args)
    elif args.cmd == "worker":
        if args.action == "start":
            worker_start(args)
        elif args.action == "stop":
            worker_stop(args)
        else:
            parser.print_help()
    elif args.cmd == "worker-process":
        worker_process_main(args)
    elif args.cmd == "status":
        cmd_status(args)
    elif args.cmd == "list":
        cmd_list(args)
    elif args.cmd == "dlq":
        if args.action == "list":
            cmd_dlq_list(args)
        elif args.action == "retry":
            cmd_dlq_retry(args)
    elif args.cmd == "config":
        cmd_config(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
