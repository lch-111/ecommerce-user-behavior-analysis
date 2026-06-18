"""
MySQL/Doris 批量数据加载 v2
从 shuju.txt 直接处理全部 318 万条数据，通过 Stream Load 写入 Doris
"""
import os
import requests
import time
from datetime import datetime, date
from collections import defaultdict

# Doris 配置
DORIS_HOST = '127.0.0.1'
DORIS_HTTP_PORT = 8040
DORIS_USER = 'root'
DORIS_PASSWORD = ''
DORIS_DB = 'ub_olap'

# 数据文件路径
DATA_FILE = 'e:/hive电商用户行为分析/code/shuju.txt'

# 年龄分组函数
def age_group(age):
    if age < 18: return '0-17'
    if age <= 25: return '18-25'
    if age <= 35: return '26-35'
    if age <= 45: return '36-45'
    return '46+'


def stream_load(table, data_lines, label_prefix='batch'):
    """通过 Doris Stream Load 批量写入 CSV 数据"""
    if not data_lines:
        print(f"  -> No data for {table}, skip")
        return 0

    url = f"http://{DORIS_HOST}:{DORIS_HTTP_PORT}/api/{DORIS_DB}/{table}/_stream_load"
    headers = {
        'Content-Type': 'text/plain; charset=UTF-8',
        'label': f"{label_prefix}_{table}_{int(time.time())}",
        'format': 'csv',
        'column_separator': ',',
    }

    csv_data = '\n'.join(data_lines)

    try:
        resp = requests.put(url, data=csv_data.encode('utf-8'),
                          headers=headers,
                          auth=(DORIS_USER, DORIS_PASSWORD),
                          timeout=300)
        result = resp.json()
        if result.get('Status') == 'Success':
            n = int(result.get('NumberLoadedRows', 0))
            print(f"  [OK] {table}: {n} rows ({len(csv_data)//1024} KB)")
            return n
        else:
            print(f"  [FAIL] {table}: {result}")
            return 0
    except Exception as e:
        print(f"  [FAIL] {table} error: {e}")
        return 0


def load_all():
    """从 shuju.txt 逐行读取，统计各种聚合 -> Doris Stream Load"""
    print(f"Reading {DATA_FILE} ...")
    print(f"Target: all ~3.18M records -> Doris")
    print()

    # 聚合统计
    province_cnt = defaultdict(int)      # province -> cnt
    behavior_cnt = defaultdict(int)      # behavior_type -> cnt
    behavior_by_gender = defaultdict(int)        # (bt, gender, age) -> cnt
    gender_cnt = defaultdict(int)        # gender -> cnt
    age_cnt = defaultdict(int)           # age -> cnt

    # 实时明细缓存 (取前 50000 条)
    realtime_samples = []

    total = 0
    start_time = time.time()

    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        next(f)  # 跳过 header

        for line in f:
            parts = line.strip().split('\t')
            if len(parts) < 9:
                continue

            # 只解析需要的字段
            behavior_type = parts[5].strip()
            if not behavior_type.isdigit():
                continue

            bt = int(behavior_type)
            age = int(parts[2].strip()) if parts[2].strip().isdigit() else 0
            gender = parts[3].strip()
            province = parts[8].strip()

            # demo1: 省份计数
            if province:
                province_cnt[province] += 1

            # demo2: 行为类型计数
            behavior_cnt[bt] += 1

            # demo7: 性别计数
            if gender:
                gender_cnt[gender] += 1

            # demo5: 年龄计数
            age_cnt[age] += 1

            # dws_behavior_daily: (behavior_type, gender, age) -> cnt
            if gender:
                key = (bt, gender, age)
                behavior_by_gender[key] += 1

            # 实时明细采样
            if len(realtime_samples) < 50000:
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                realtime_samples.append(
                    f"{now_str},{parts[1].strip()},{age},{gender},{province},{bt},{now_str}"
                )

            total += 1
            if total % 500000 == 0:
                elapsed = time.time() - start_time
                print(f"  Progress: {total:,} lines ({total/elapsed:.0f} lines/s)")

    elapsed = time.time() - start_time
    print(f"\nParsed {total:,} lines in {elapsed:.1f}s")
    print()

    # ========== 写入 Doris ==========
    today = date.today().isoformat()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    all_loaded = 0

    # 1. dws_province_daily: 省份日汇总
    print("[1/4] dws_province_daily (省份日汇总)...")
    lines = []
    for prov, cnt in sorted(province_cnt.items(), key=lambda x: -x[1]):
        lines.append(f"{prov},{today},{cnt},0,0")
    # Stream Load 分批 (Doris 单次建议 <100MB)
    BATCH = 20000
    for i in range(0, len(lines), BATCH):
        chunk = lines[i:i+BATCH]
        all_loaded += stream_load('dws_province_daily', chunk, 'province')

    # 2. dws_behavior_daily: 行为日汇总
    print("\n[2/4] dws_behavior_daily (行为+性别+年龄汇总)...")
    lines = []
    for (bt, gender, age), cnt in behavior_by_gender.items():
        lines.append(f"{bt},{gender},{age},{today},{cnt}")
    for i in range(0, len(lines), BATCH):
        chunk = lines[i:i+BATCH]
        all_loaded += stream_load('dws_behavior_daily', chunk, 'behavior')

    # 3. dwd_user_behavior_realtime: 实时明细（前 50000 条）
    print("\n[3/4] dwd_user_behavior_realtime (实时明细采样)...")
    if realtime_samples:
        all_loaded += stream_load('dwd_user_behavior_realtime', realtime_samples, 'realtime')

    # 4. 验证
    print("\n[4/4] Verifying Doris data...")
    try:
        for table in ['dws_province_daily', 'dws_behavior_daily', 'dwd_user_behavior_realtime']:
            verify_url = f"http://{DORIS_HOST}:{DORIS_HTTP_PORT}/api/{DORIS_DB}/{table}/_count"
            r = requests.get(verify_url, auth=(DORIS_USER, DORIS_PASSWORD), timeout=10)
            print(f"  {table}: {r.json()}")
    except Exception as e:
        print(f"  Verify error: {e}")

    print(f"\n{'='*50}")
    print(f"Done! {all_loaded:,} total rows loaded to Doris")
    print(f"Source: {total:,} raw records processed")
    print(f"{'='*50}")
    return all_loaded


if __name__ == '__main__':
    load_all()
