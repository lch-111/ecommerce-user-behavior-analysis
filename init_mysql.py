"""
初始化 MySQL 数据库 ubanalysis
从 shuju.txt 导入数据到 demo1-demo7 表
"""
import pymysql
import os
import re
from datetime import datetime

# MySQL 配置
MYSQL_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '123168',
    'charset': 'utf8mb4'
}

DB_NAME = 'ubanalysis'
DATA_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'code', 'shuju.txt')


def get_conn():
    return pymysql.connect(**MYSQL_CONFIG)


def create_database_and_tables(conn):
    cursor = conn.cursor()

    # 创建数据库
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARSET utf8mb4")
    cursor.execute(f"USE `{DB_NAME}`")

    # demo1: 省份访问量（聚合数据）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS demo1 (
            province VARCHAR(50) NOT NULL,
            cnt BIGINT NOT NULL,
            PRIMARY KEY (province)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # demo2: 用户行为类型（明细）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS demo2 (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            behavior_type INT NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # demo3: 近期访问日期（明细）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS demo3 (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            `date` VARCHAR(20) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # demo4: 用户消费明细
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS demo4 (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(50) NOT NULL,
            behavior_type INT NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # demo5: 年龄分布（明细）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS demo5 (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            age INT NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # demo6: 每日各性别访问明细
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS demo6 (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            `date` VARCHAR(20) NOT NULL,
            gender VARCHAR(10) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # demo7: 用户性别（明细）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS demo7 (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            gender VARCHAR(10) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    conn.commit()
    print("Tables created successfully")


def load_data(conn):
    cursor = conn.cursor()
    cursor.execute(f"USE `{DB_NAME}`")

    # 先清除已有数据
    for t in ['demo1', 'demo2', 'demo3', 'demo4', 'demo5', 'demo6', 'demo7']:
        cursor.execute(f"TRUNCATE TABLE {t}")
    conn.commit()

    # 解析数据
    print(f"Reading data from {DATA_FILE} ...")

    # 统计变量
    total = 0
    province_counter = {}
    batch_size = 50000

    # 批量插入缓存
    batch_demo2 = []
    batch_demo3 = []
    batch_demo4 = []
    batch_demo5 = []
    batch_demo6 = []
    batch_demo7 = []

    start_time = datetime.now()

    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        # 跳过首行（表头）
        header = f.readline()

        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 9:
                continue

            try:
                # id = parts[0]
                user_id = parts[1].strip()
                age = parts[2].strip()
                gender = parts[3].strip()
                # item_id = parts[4]
                behavior_type = parts[5].strip()
                # item_category = parts[6]
                time_val = parts[7].strip()
                province = parts[8].strip()

                # 处理日期格式: "2014-12-22" -> "12-22"
                if '-' in time_val:
                    date_parts = time_val.split('-')
                    if len(date_parts) >= 3:
                        date_mmdd = f"{date_parts[1]}-{date_parts[2]}"
                    elif len(date_parts) == 2:
                        date_mmdd = time_val
                    else:
                        date_mmdd = time_val
                else:
                    date_mmdd = time_val

                # demo1: 统计省份
                if province:
                    province_counter[province] = province_counter.get(province, 0) + 1

                # demo2
                if behavior_type and behavior_type.isdigit():
                    batch_demo2.append(int(behavior_type))

                # demo3
                if date_mmdd:
                    batch_demo3.append(date_mmdd)

                # demo4
                if user_id and behavior_type and behavior_type.isdigit():
                    batch_demo4.append((user_id, int(behavior_type)))

                # demo5
                if age and age.isdigit():
                    batch_demo5.append(int(age))

                # demo6
                if date_mmdd and gender:
                    batch_demo6.append((date_mmdd, gender))

                # demo7
                if gender:
                    batch_demo7.append(gender)

                total += 1

                # 批量写入
                if total % batch_size == 0:
                    _flush_batches(cursor, batch_demo2, batch_demo3, batch_demo4,
                                   batch_demo5, batch_demo6, batch_demo7)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = total / elapsed if elapsed > 0 else 0
                    print(f"  Processed {total:,} lines | {rate:.0f} lines/s")

            except (ValueError, IndexError) as e:
                continue

    # 刷剩余数据
    _flush_batches(cursor, batch_demo2, batch_demo3, batch_demo4,
                   batch_demo5, batch_demo6, batch_demo7)

    # 写入 demo1（聚合数据）
    print("Writing demo1 (province aggregation)...")
    demo1_data = [(prov, cnt) for prov, cnt in province_counter.items()]
    demo1_data.sort(key=lambda x: x[1], reverse=True)

    chunk_size = 1000
    for i in range(0, len(demo1_data), chunk_size):
        chunk = demo1_data[i:i+chunk_size]
        cursor.executemany(
            "INSERT INTO demo1 (province, cnt) VALUES (%s, %s)",
            chunk
        )

    conn.commit()

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nDone! Total: {total:,} records in {elapsed:.1f}s")
    print(f"  demo1: {len(demo1_data)} provinces")
    print(f"  demo2: {len(province_counter)} records (same count as total for behavior)")

    # 验证各表数据量
    for t in ['demo1', 'demo2', 'demo3', 'demo4', 'demo5', 'demo6', 'demo7']:
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        count = cursor.fetchone()[0]
        print(f"  {t}: {count:,} rows")


def _flush_batches(cursor, b2, b3, b4, b5, b6, b7):
    """批量写入数据"""
    if b2:
        cursor.executemany("INSERT INTO demo2 (behavior_type) VALUES (%s)",
                           [(x,) for x in b2])
        b2.clear()
    if b3:
        cursor.executemany("INSERT INTO demo3 (`date`) VALUES (%s)",
                           [(x,) for x in b3])
        b3.clear()
    if b4:
        cursor.executemany("INSERT INTO demo4 (user_id, behavior_type) VALUES (%s, %s)", b4)
        b4.clear()
    if b5:
        cursor.executemany("INSERT INTO demo5 (age) VALUES (%s)",
                           [(x,) for x in b5])
        b5.clear()
    if b6:
        cursor.executemany("INSERT INTO demo6 (`date`, gender) VALUES (%s, %s)", b6)
        b6.clear()
    if b7:
        cursor.executemany("INSERT INTO demo7 (gender) VALUES (%s)",
                           [(x,) for x in b7])
        b7.clear()
    cursor.connection.commit()


if __name__ == '__main__':
    conn = get_conn()
    try:
        create_database_and_tables(conn)
        load_data(conn)
    finally:
        conn.close()
