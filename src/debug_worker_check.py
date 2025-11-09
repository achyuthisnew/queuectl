from src.storage import Storage
from src.utils import logger

s = Storage()
jobs = s.list_jobs()

logger.info(f"Database path: {s.db_path}")
logger.info(f"Total jobs found: {len(jobs)}")

for j in jobs:
    logger.info(f"Job {j['id']} - state={j['state']} attempts={j['attempts']}")
