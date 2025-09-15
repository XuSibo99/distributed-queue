import redis
import time

r = redis.Redis(host="redis", port=6379, decode_responses=True)

def run_worker():
    while True:
        job_id = r.rpop("queue:default:ready")
        if job_id:
            payload = r.hget(f"job:{job_id}", "payload")
            print(f"Processing {job_id} → {payload}", flush=True)
            time.sleep(1)
        else:
            time.sleep(0.5)

if __name__ == "__main__":
    run_worker()