
from fastapi import FastAPI
import redis
import uuid
import time

app = FastAPI()
r = redis.Redis(host="redis", port=6379, decode_responses=True)

@app.post("/enqueue")
def enqueue(payload: dict):
    job_id = str(uuid.uuid4())
    job_key = f"job:{job_id}"
    r.hset(job_key, mapping={"payload": str(payload), "created_at": str(time.time())})
    r.lpush("queue:default:ready", job_id)
    return {"job_id": job_id}

@app.get("/dequeue")
def dequeue():
    job_id = r.rpop("queue:default:ready")
    if not job_id:
        return {"job_id": None}
    return {"job_id": job_id, "payload": r.hget(job_id, "payload")}
