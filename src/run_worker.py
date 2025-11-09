from src.worker import WorkerManager

if __name__ == "__main__":
    manager = WorkerManager()
    manager.start_workers(1)
