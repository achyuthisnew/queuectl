# 🚀 QueueCTL - CLI-Based Background Job Queue System

**QueueCTL** is a production-grade, CLI-driven background job queue system built using Python.  
It supports job scheduling, retries with exponential backoff, multiple workers, and a **Dead Letter Queue (DLQ)** for permanently failed jobs.

---

## 🧠 Features

✅ Enqueue and persist background jobs  
✅ Process jobs using multiple worker processes  
✅ Exponential backoff retry mechanism  
✅ Dead Letter Queue (DLQ) for failed jobs  
✅ Persistent storage using SQLite  
✅ CLI-based configuration and status management  
✅ Graceful worker shutdown  
✅ Optional logging of job outputs  

---

## 🧩 Tech Stack

- **Language:** Python 3.10+
- **Libraries:** `click`, `sqlite3`, `multiprocessing`
- **Persistence:** SQLite
- **CLI Framework:** Click

---

## ⚙️ Setup Instructions

### 1️⃣ Clone Repository

```bash
git clone https://github.com/achyuthisnew/queuectl.git
cd queuectl
```

2️⃣ Install Dependencies
pip install -r requirements.txt


or install in editable mode (for development):

python -m pip install --user -e .

🧪 Usage Examples
▶️ Enqueue Jobs
queuectl enqueue "{\"id\":\"job1\",\"command\":\"echo Hello QueueCTL\"}"
queuectl enqueue "{\"id\":\"job2\",\"command\":\"echo Processing background jobs\"}"

📊 View Queue Status
queuectl status

⚙️ Start Workers
python -m src.cli worker start --count 1

🛑 Stop Workers
queuectl worker stop

💀 View Dead Letter Queue
queuectl dlq list

♻️ Retry from DLQ
queuectl dlq retry job1
