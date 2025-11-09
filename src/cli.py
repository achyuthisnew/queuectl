"""CLI interface for QueueCTL."""

import json
import sys

import click

from .config import Config
from .job_manager import JobManager
from .storage import Storage
from .worker import WorkerManager


@click.group()
def cli():
    """QueueCTL - Production-grade CLI-based background job queue system."""
    pass


@cli.command()
@click.argument('job_json')
def enqueue(job_json):
    """
    Enqueue a new job.
    
    JOB_JSON: JSON string containing job data (must have 'id' and 'command')
    
    Example: queuectl enqueue '{"id":"job1","command":"sleep 2"}'
    """
    try:
        job_data = json.loads(job_json)
        manager = JobManager()
        job = manager.enqueue(job_data)
        
        click.echo(f"✓ Job enqueued successfully")
        click.echo(f"  ID: {job['id']}")
        click.echo(f"  Command: {job['command']}")
        click.echo(f"  State: {job['state']}")
    except json.JSONDecodeError:
        click.echo("✗ Error: Invalid JSON format", err=True)
        sys.exit(1)
    except ValueError as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def worker():
    """Manage worker processes."""
    pass


@worker.command()
@click.option('--count', default=3, help='Number of workers to start')
def start(count):
    """Start worker processes."""
    try:
        manager = WorkerManager()
        manager.start_workers(count)
        click.echo(f"✓ Started {count} workers")
    except RuntimeError as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)


@worker.command()
def stop():
    """Stop all worker processes."""
    try:
        manager = WorkerManager()
        manager.stop_workers()
        click.echo("✓ Workers stopped")
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
def status():
    """Show queue status."""
    try:
        manager = JobManager()
        status = manager.get_status()
        
        click.echo("Queue Status:")
        click.echo("─" * 40)
        for state, count in status.items():
            click.echo(f"  {state.capitalize():12s}: {count:5d}")
        click.echo("─" * 40)
        click.echo(f"  {'Total':12s}: {sum(status.values()):5d}")
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)


# ============================================================================
# Additional CLI Commands
# ============================================================================

@cli.command()
@click.option('--state', default=None, help='Filter jobs by state (pending, processing, completed, failed, dead)')
def list(state):
    """List jobs in the queue."""
    try:
        manager = JobManager()
        jobs = manager.list_jobs(state)

        if not jobs:
            click.echo("No jobs found.")
            return

        click.echo(f"{'ID':<20} {'STATE':<12} {'ATTEMPTS':<9} {'COMMAND'}")
        click.echo("-" * 70)
        for job in jobs:
            click.echo(f"{job['id']:<20} {job['state']:<12} {job['attempts']:<9} {job['command']}")
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def dlq():
    """Dead Letter Queue operations."""
    pass


@dlq.command('list')
def dlq_list():
    """List jobs in the Dead Letter Queue."""
    try:
        manager = JobManager()
        jobs = manager.list_jobs('dead')

        if not jobs:
            click.echo("No jobs in Dead Letter Queue.")
            return

        click.echo(f"{'ID':<20} {'ATTEMPTS':<9} {'COMMAND'}")
        click.echo("-" * 70)
        for job in jobs:
            click.echo(f"{job['id']:<20} {job['attempts']:<9} {job['command']}")
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)


@dlq.command('retry')
@click.argument('job_id')
def dlq_retry(job_id):
    """Retry a specific job from the Dead Letter Queue."""
    try:
        manager = JobManager()
        manager.retry_from_dlq(job_id)
        click.echo(f"✓ Job {job_id} retried from DLQ.")
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def config():
    """Manage QueueCTL configuration."""
    pass


@config.command('set')
@click.argument('key')
@click.argument('value')
def config_set(key, value):
    """Set a configuration key."""
    try:
        cfg = Config()
        # Try to convert numeric values
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                pass
        cfg.set(key, value)
        click.echo(f"✓ Configuration updated: {key} = {value}")
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)


@config.command('show')
def config_show():
    """Display current configuration."""
    try:
        cfg = Config()
        data = cfg.get_all()
        click.echo("Current Configuration:")
        click.echo("-" * 40)
        for k, v in data.items():
            click.echo(f"{k:20s}: {v}")
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)
