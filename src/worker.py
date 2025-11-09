"""Worker process implementation."""

import multiprocessing
import os
import signal
import subprocess
import time
import uuid
from pathlib import Path
from typing import Optional

from .config import Config
from .job_manager import JobManager
from .storage import Storage
from .utils import logger, setup_logging


class Worker:
    """
    Worker process that executes jobs from the queue.
    """

    def __init__(
        self,
        worker_id: str,
        storage: Optional[Storage] = None,
        config: Optional[Config] = None
    ):
        """
        Initialize worker.

        Args:
            worker_id: Unique worker identifier
            storage: Storage instance
            config: Config instance
        """
        self.worker_id = worker_id
        self.storage = storage or Storage()
        self.config = config or Config()
        self.job_manager = JobManager(self.storage, self.config)
        self.should_stop = False
        self.current_job_id = None

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Worker {self.worker_id} received shutdown signal")
        self.should_stop = True

    def _execute_command(self, command: str, job_id: str) -> tuple[bool, str]:
        """
        Execute a shell command.

        Args:
            command: Shell command to execute
            job_id: Current job ID (for logging)

        Returns:
            Tuple of (success, output/error message)
        """
        try:
            # Optional logging of stdout/stderr to file
            log_dir = Path.home() / ".queuectl" / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / f"{job_id}.log"

            with open(log_path, "w", encoding="utf-8") as log_file:
                result = subprocess.run(
                    command,
                    shell=True,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=300  # 5 minutes
                )

            if result.returncode == 0:
                return True, f"Output logged to {log_path}"
            else:
                return False, f"Non-zero exit code {result.returncode}. See {log_path}"

        except subprocess.TimeoutExpired:
            return False, f"Command timeout. See log at {log_path}"
        except Exception as e:
            return False, str(e)

    def _process_job(self, job: dict) -> None:
        """Process a single job."""
        job_id = job["id"]
        self.current_job_id = job_id

        logger.info(
            f"Worker {self.worker_id} processing job {job_id}: {job['command']}"
        )

        # Execute the command
        success, output = self._execute_command(job["command"], job_id)

        if success:
            self.job_manager.mark_completed(job_id)
            logger.info(f"Worker {self.worker_id} completed job {job_id}")
        else:
            error_msg = f"Command failed: {output}"
            self.job_manager.mark_failed(job_id, error_msg)
            logger.error(
                f"Worker {self.worker_id} failed job {job_id}: {error_msg}"
            )

        # Release lock
        self.storage.release_job_lock(job_id, self.worker_id)
        self.current_job_id = None

    def run(self) -> None:
        """Main worker loop."""
        logger.info(f"Worker {self.worker_id} started")
        poll_interval = self.config.get("worker_poll_interval")

        while not self.should_stop:
            try:
                ready_jobs = self.storage.get_ready_jobs(limit=1)
                logger.info(f"Worker {self.worker_id} found {len(ready_jobs)} ready jobs")

                if not ready_jobs:
                    time.sleep(poll_interval)
                    continue

                job = ready_jobs[0]
                if self.storage.acquire_job_lock(job["id"], self.worker_id):
                    job = self.storage.get_job(job["id"])
                    self._process_job(job)
                else:
                    time.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")
                time.sleep(poll_interval)

        if self.current_job_id:
            logger.info(
                f"Worker {self.worker_id} releasing lock on job {self.current_job_id}"
            )
            self.storage.release_job_lock(self.current_job_id, self.worker_id)

        logger.info(f"Worker {self.worker_id} stopped")


def worker_process(worker_id: str):
    """Entry point for worker process."""
    setup_logging()
    worker = Worker(worker_id)
    worker.run()


class WorkerManager:
    """
    Manages multiple worker processes.
    """

    def __init__(self):
        """Initialize worker manager."""
        self.processes = []
        self.pid_file = Path.home() / ".queuectl" / "workers.pid"

        # ✅ Windows-safe multiprocessing initialization
        try:
            multiprocessing.set_start_method("spawn", force=True)
        except RuntimeError:
            pass  # already set

    def start_workers(self, count: int) -> None:
        """Start worker processes."""
        if self._are_workers_running():
            raise RuntimeError("Workers are already running")

        logger.info(f"Starting {count} workers")

        # ✅ Use spawn context explicitly (Windows compatibility)
        ctx = multiprocessing.get_context("spawn")
        pids = []

        for i in range(count):
            worker_id = f"worker-{i}-{uuid.uuid4().hex[:8]}"
            process = ctx.Process(
                target=worker_process,
                args=(worker_id,)
            )
            process.start()
            self.processes.append(process)
            pids.append(process.pid)
            logger.info(f"Started worker {worker_id} (PID: {process.pid})")

        self._save_pids(pids)
        logger.info(f"All {count} workers started")

    def stop_workers(self) -> None:
        """Stop all running worker processes."""
        pids = self._load_pids()

        if not pids:
            logger.warning("No running workers found")
            return

        logger.info(f"Stopping {len(pids)} workers")

        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
                logger.info(f"Sent SIGTERM to worker PID {pid}")
            except ProcessLookupError:
                logger.warning(f"Worker PID {pid} not found")
            except Exception as e:
                logger.error(f"Error stopping worker PID {pid}: {e}")

        time.sleep(2)

        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        if self.pid_file.exists():
            self.pid_file.unlink()

        logger.info("All workers stopped")

    def _save_pids(self, pids: list) -> None:
        """Save worker PIDs to file."""
        self.pid_file.parent.mkdir(exist_ok=True)
        with open(self.pid_file, "w") as f:
            for pid in pids:
                f.write(f"{pid}\n")

    def _load_pids(self) -> list:
        """Load worker PIDs from file."""
        if not self.pid_file.exists():
            return []

        with open(self.pid_file, "r") as f:
            return [int(line.strip()) for line in f if line.strip()]

    def _are_workers_running(self) -> bool:
        """Check if any workers are running."""
        pids = self._load_pids()

        for pid in pids:
            try:
                os.kill(pid, 0)
                return True
            except ProcessLookupError:
                continue

        return False





