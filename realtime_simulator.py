"""
实时流模拟器 v3 — 直接从 shuju.txt 读取，速率限制，每分钟聚合写入 Doris
效果：大屏上数据每 60 秒刷新一次，模拟真正的流处理
"""
import time
import requests
import threading
from datetime import datetime, timedelta
from collections import defaultdict

# ====== 配置 ======
DORIS_HOST = '127.0.0.1'
DORIS_HTTP_PORT = 8040
DORIS_DB = 'ub_olap'
DORIS_TABLE = 'dws_realtime_minute'

DATA_FILE = 'e:/hive电商用户行为分析/code/shuju.txt'
TARGET_RATE = 500  # 每秒发送条数

running = True
minute_buffer = defaultdict(int)  # (province, bt, gender, age_group) -> count
buffer_lock = threading.Lock()


def age_group(age):
    if age < 18: return '0-17'
    if age <= 25: return '18-25'
    if age <= 35: return '26-35'
    if age <= 45: return '36-45'
    return '46+'


def stream_data():
    """从 shuju.txt 逐行读取，填充缓冲区，控制速率"""
    global running
    print(f"[Stream] Reading with rate limit ~{TARGET_RATE}/s ...")

    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        next(f)  # skip header
        lines = f.readlines()

    print(f"[Stream] Loaded {len(lines):,} lines, starting replay...")

    sent = 0
    start = time.time()
    batch = []

    while running:
        for line in lines:
            if not running:
                break
            parts = line.strip().split('\t')
            if len(parts) < 9:
                continue
            if not parts[5].strip().isdigit():
                continue

            province = parts[8].strip()
            bt = int(parts[5])
            gender = parts[3].strip()
            age = int(parts[2]) if parts[2].strip().isdigit() else 0

            key = (province, bt, gender, age_group(age))
            with buffer_lock:
                minute_buffer[key] += 1

            sent += 1
            batch.append(1)

            # 控制速率
            if len(batch) >= TARGET_RATE:
                elapsed = time.time() - start
                expected = sent / TARGET_RATE
                if elapsed < expected:
                    time.sleep(expected - elapsed)
                batch = []

            # 每 5 万条打印一次进度
            if sent % 50000 == 0:
                elapsed = time.time() - start
                print(f"[Stream] {sent:,} events replayed | {sent/elapsed:.0f}/s")

        # 全部发完一轮后，继续从头循环（模拟持续流）
        if not running:
            break
        print(f"[Stream] Round complete ({sent:,} total), restarting from top...")

    print(f"[Stream] Stopped. Total: {sent:,}")


def flush_to_doris():
    """将缓冲区数据写入 Doris"""
    with buffer_lock:
        if not minute_buffer:
            return 0
        batch = dict(minute_buffer)
        minute_buffer.clear()

    minute_key = datetime.now().strftime('%Y-%m-%d %H:%M:00')
    label_minute = minute_key.replace(' ', 'T').replace(':', '')
    lines = []
    for (province, bt, gender, ag), cnt in batch.items():
        lines.append(f"{province},{bt},{gender},{ag},{minute_key},{cnt}")

    url = f"http://{DORIS_HOST}:{DORIS_HTTP_PORT}/api/{DORIS_DB}/{DORIS_TABLE}/_stream_load"
    headers = {
        'Content-Type': 'text/plain; charset=UTF-8',
        'label': f"rt_{label_minute}_{int(time.time())}",
        'format': 'csv',
        'column_separator': ',',
    }
    csv_data = '\n'.join(lines)

    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = requests.put(url, data=csv_data.encode('utf-8'),
                              headers=headers, auth=('root', ''), timeout=30)
            result = resp.json()
            if result.get('Status') == 'Success':
                n = int(result.get('NumberLoadedRows', 0))
                total = sum(batch.values())
                print(f"[Doris] {minute_key} | {n} rows | {total:,} events/sec")
                return n
            else:
                # 标签冲突已存在是正常情况（重试写入同一分钟），不打印错误
                if 'LABEL_ALREADY_EXISTS' in str(result):
                    return 0
                print(f"[Doris] FAIL (attempt {attempt+1}): {result.get('Message','')[:60]}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return 0
        except Exception as e:
            print(f"[Doris] Error (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 指数退避: 1s, 2s, 4s
                continue
            return 0


def scheduler():
    """每分钟执行一次 flush_to_doris"""
    global running
    print("[Scheduler] Starting...")
    while running:
        # 计算到下一个 :10 秒的等待时间
        now = datetime.now()
        if now.second < 10:
            wait = 10 - now.second
        else:
            wait = 70 - now.second  # 下一分钟的 10 秒
        time.sleep(wait)
        if running:
            flush_to_doris()


def main():
    global running
    print("=" * 50)
    print("Realtime Stream Simulator v3")
    print("  shuju.txt -> Minute Agg (in memory) -> Doris")
    print(f"  Rate: ~{TARGET_RATE}/s | Flush: every 60s")
    print("=" * 50)

    t1 = threading.Thread(target=stream_data, daemon=True)
    t2 = threading.Thread(target=scheduler, daemon=True)

    t1.start()
    t2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        running = False
        time.sleep(2)
        flush_to_doris()
        print("Done.")


if __name__ == '__main__':
    main()
