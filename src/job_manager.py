"""Job management and state transitions."""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .config import Config
from .storage import Storage
from .utils import calculate_backoff_delay, get_timestamp, logger


class JobManager:
    """
    Manages job lifecycle, state transitions, and retry logic.
    """
    
    VALID_STATES = ['pending', 'processing', 'completed', 'failed', 'dead']
    
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Config] = None):
        """
        Initialize job manager.
        
        Args:
            storage: Storage instance (creates new if None)
            config: Config instance (creates new if None)
        """
        self.storage = storage or Storage()
        self.config = config or Config()
    
    def enqueue(self, job_data: Dict) -> Dict:
        """
        Enqueue a new job.
        
        Args:
            job_data: Job data containing at least 'id' and 'command'
            
        Returns:
            Created job dictionary
            
        Raises:
            ValueError: If job data is invalid or job ID already exists
        """
        # Validate required fields
        if 'id' not in job_data or 'command' not in job_data:
            raise ValueError("Job must have 'id' and 'command' fields")
        
        # Check for duplicate ID
        existing = self.storage.get_job(job_data['id'])
        if existing:
            raise ValueError(f"Job with ID '{job_data['id']}' already exists")
        
        # Create job with defaults
        now = get_timestamp()
        job = {
            'id': job_data['id'],
            'command': job_data['command'],
            'state': 'pending',
            'attempts': 0,
            'max_retries': job_data.get('max_retries', self.config.get('max_retries')),
            'created_at': now,
            'updated_at': now,
            'scheduled_at': job_data.get('scheduled_at'),
            'error_message': None,
            'lock_id': None
        }
        
        self.storage.create_job(job)
        logger.info(f"Job enqueued: {job['id']}")
        
        return job
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """
        Get job by ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job dictionary or None
        """
        return self.storage.get_job(job_id)
    
    def list_jobs(self, state: Optional[str] = None) -> List[Dict]:
        """
        List jobs, optionally filtered by state.
        
        Args:
            state: Filter by job state
            
        Returns:
            List of job dictionaries
        """
        if state and state not in self.VALID_STATES:
            raise ValueError(f"Invalid state: {state}")
        
        return self.storage.list_jobs(state)
    
    def mark_completed(self, job_id: str) -> None:
        """
        Mark a job as completed.
        
        Args:
            job_id: Job identifier
        """
        self.storage.update_job(job_id, {
            'state': 'completed',
            'error_message': None
        })
        logger.info(f"Job completed: {job_id}")
    
    def mark_failed(self, job_id: str, error_message: str) -> None:
        """
        Mark a job as failed and handle retry logic.
        
        Args:
            job_id: Job identifier
            error_message: Error description
        """
        job = self.storage.get_job(job_id)
        if not job:
            logger.error(f"Job not found: {job_id}")
            return
        
        attempts = job['attempts'] + 1
        max_retries = job['max_retries']
        
        if attempts >= max_retries:
            # Move to dead letter queue
            self.storage.update_job(job_id, {
                'state': 'dead',
                'attempts': attempts,
                'error_message': error_message,
                'lock_id': None
            })
            logger.warning(f"Job moved to DLQ after {attempts} attempts: {job_id}")
        else:
            # Schedule retry with exponential backoff
            backoff_base = self.config.get('backoff_base')
            delay = calculate_backoff_delay(attempts, backoff_base)
            scheduled_at = (
                datetime.utcnow() + timedelta(seconds=delay)
            ).isoformat()
            
            self.storage.update_job(job_id, {
                'state': 'pending',
                'attempts': attempts,
                'error_message': error_message,
                'scheduled_at': scheduled_at,
                'lock_id': None
            })
            logger.info(
                f"Job scheduled for retry #{attempts} in {delay}s: {job_id}"
            )
    
    def retry_from_dlq(self, job_id: str) -> None:
        """
        Retry a job from the dead letter queue.
        
        Args:
            job_id: Job identifier
            
        Raises:
            ValueError: If job is not in DLQ
        """
        job = self.storage.get_job(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")
        
        if job['state'] != 'dead':
            raise ValueError(f"Job is not in DLQ: {job_id}")
        
        self.storage.update_job(job_id, {
            'state': 'pending',
            'attempts': 0,
            'error_message': None,
            'scheduled_at': None,
            'lock_id': None
        })
        logger.info(f"Job retried from DLQ: {job_id}")
    
    def get_status(self) -> Dict[str, int]:
        """
        Get job count by state.
        
        Returns:
            Dictionary mapping state to count
        """
        status = {state: 0 for state in self.VALID_STATES}
        
        for state in self.VALID_STATES:
            jobs = self.storage.list_jobs(state)
            status[state] = len(jobs)
        
        return status
