# 引き落とし API へ毎秒およそ1件の要求を送り続ける検証用スクリプト
import json, random, sys, time, uuid, urllib.request, datetime
URL = sys.argv[1]
RATE = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
recent = []
def post(key, body):
    req = urllib.request.Request(URL, data=json.dumps(body).encode(), method="POST", headers={"Idempotency-Key": key, "Content-Type": "application/json"})
    t = time.time()
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            code = r.status
    except urllib.error.HTTPError as e:
        code = e.code
    except Exception as e:
        code = type(e).__name__
    return code, int((time.time() - t) * 1000)
while True:
    p = random.random()
    if p < 0.08 and recent:
        key, body = random.choice(recent)  # クライアントの再送
        kind = "retry"
    elif p < 0.10:
        key, body = str(uuid.uuid4()), {"accountId": "A003", "amount": 1000}  # 残高不足
        kind = "insufficient"
    else:
        key, body = str(uuid.uuid4()), {"accountId": random.choice(["A001", "A002"]), "amount": random.randint(100, 5000)}
        kind = "new"
        recent = (recent + [(key, body)])[-50:]
    code, ms = post(key, body)
    print(datetime.datetime.now().strftime("%H:%M:%S"), kind, code, ms, flush=True)
    time.sleep(random.expovariate(RATE))
