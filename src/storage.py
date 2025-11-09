"""Persistent storage layer using SQLite."""

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional

from .utils import get_timestamp, logger


class Storage:
    """
    SQLite-based persistent storage for jobs.
    Thread-safe with connection pooling per thread.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize storage layer.
        
        Args:
            db_path: Path to SQLite database file
        """
        if db_path is None:
            data_dir = Path.home() / ".queuectl"
            data_dir.mkdir(exist_ok=True)
            db_path = str(data_dir / "jobs.db")
        
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()
    
    def _get_connection(self) -> sqlite3.Connection:
        """
        Get thread-local database connection.
        
        Returns:
            SQLite connection
        """
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False
            )
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn
    
    @contextmanager
    def _get_cursor(self):
        """
        Context manager for database cursor with auto-commit.
        
        Yields:
            SQLite cursor
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cursor.close()
    
    def _init_db(self) -> None:
        """Initialize database schema."""
        with self._get_cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    scheduled_at TEXT,
                    error_message TEXT,
                    lock_id TEXT
                )
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_state 
                ON jobs(state)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_scheduled_at 
                ON jobs(scheduled_at)
            """)
    
    def create_job(self, job: Dict) -> None:
        """
        Create a new job in the database.
        
        Args:
            job: Job dictionary with all required fields
        """
        with self._get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO jobs 
                (id, command, state, attempts, max_retries, 
                 created_at, updated_at, scheduled_at, error_message, lock_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job['id'],
                job['command'],
                job['state'],
                job.get('attempts', 0),
                job.get('max_retries', 3),
                job['created_at'],
                job['updated_at'],
                job.get('scheduled_at'),
                job.get('error_message'),
                job.get('lock_id')
            ))
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """
        Get job by ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job dictionary or None if not found
        """
        with self._get_cursor() as cursor:
            cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def update_job(self, job_id: str, updates: Dict) -> None:
        """
        Update job fields.
        
        Args:
            job_id: Job identifier
            updates: Dictionary of fields to update
        """
        updates['updated_at'] = get_timestamp()
        
        set_clause = ", ".join(f"{key} = ?" for key in updates.keys())
        values = list(updates.values()) + [job_id]
        
        with self._get_cursor() as cursor:
            cursor.execute(
                f"UPDATE jobs SET {set_clause} WHERE id = ?",
                values
            )
    
    def list_jobs(self, state: Optional[str] = None) -> List[Dict]:
        """
        List jobs, optionally filtered by state.
        
        Args:
            state: Filter by job state (optional)
            
        Returns:
            List of job dictionaries
        """
        with self._get_cursor() as cursor:
            if state:
                cursor.execute(
                    "SELECT * FROM jobs WHERE state = ? ORDER BY created_at",
                    (state,)
                )
            else:
                cursor.execute("SELECT * FROM jobs ORDER BY created_at")
            
            return [dict(row) for row in cursor.fetchall()]
    
    def acquire_job_lock(self, job_id: str, lock_id: str) -> bool:
        """
        Attempt to acquire a lock on a job.
        
        Args:
            job_id: Job identifier
            lock_id: Unique lock identifier
            
        Returns:
            True if lock acquired, False otherwise
        """
        with self._get_cursor() as cursor:
            cursor.execute("""
                UPDATE jobs 
                SET lock_id = ?, state = 'processing', updated_at = ?
                WHERE id = ? AND (lock_id IS NULL OR lock_id = '')
                  AND state = 'pending'
            """, (lock_id, get_timestamp(), job_id))
            
            return cursor.rowcount > 0
    
    def release_job_lock(self, job_id: str, lock_id: str) -> None:
        """
        Release a lock on a job.
        
        Args:
            job_id: Job identifier
            lock_id: Lock identifier to verify ownership
        """
        with self._get_cursor() as cursor:
            cursor.execute("""
                UPDATE jobs 
                SET lock_id = NULL, updated_at = ?
                WHERE id = ? AND lock_id = ?
            """, (get_timestamp(), job_id, lock_id))
    
    def get_ready_jobs(self, limit: int = 100) -> List[Dict]:
        """
        Get jobs ready for processing (pending and scheduled time passed).
        
        Args:
            limit: Maximum number of jobs to return
            
        Returns:
            List of job dictionaries
        """
        current_time = get_timestamp()
        
        with self._get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM jobs 
                WHERE state = 'pending' 
                  AND (scheduled_at IS NULL OR scheduled_at <= ?)
                ORDER BY created_at
                LIMIT ?
            """, (current_time, limit))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def delete_job(self, job_id: str) -> None:
        """
        Delete a job from the database.
        
        Args:
            job_id: Job identifier
        """
        with self._get_cursor() as cursor:
            cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
