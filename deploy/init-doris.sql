-- ============================================
-- Doris 初始化 SQL: 建库 + 建表
-- 使用方式: mysql -h127.0.0.1 -P9030 -uroot < init-doris.sql
-- ============================================

-- 创建分析库
CREATE DATABASE IF NOT EXISTS ub_olap;

USE ub_olap;

-- ========================
-- 1. 实时明细表 (Duplicate Key)
-- Flink 实时写入的原始事件
-- ========================
CREATE TABLE IF NOT EXISTS dwd_user_behavior_realtime (
    event_time   DATETIME NOT NULL,
    user_id      VARCHAR(50) NOT NULL,
    age          INT NOT NULL,
    gender       VARCHAR(10) NOT NULL,
    province     VARCHAR(50) NOT NULL,
    behavior_type INT NOT NULL COMMENT '1=浏览 2=收藏 3=加购 4=购买',
    load_time    DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(event_time, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD",
    "compaction_policy" = "time_series"
);

-- ========================
-- 2. 分钟级聚合表 (Aggregate Key)
-- Flink 每分钟写入的实时聚合
-- ========================
CREATE TABLE IF NOT EXISTS dws_realtime_minute (
    province      VARCHAR(50) NOT NULL,
    behavior_type INT NOT NULL,
    gender        VARCHAR(10) NOT NULL,
    age_group     VARCHAR(20) NOT NULL COMMENT '18-25,26-35,36-45,46+',
    minute_key    DATETIME NOT NULL,
    cnt           BIGINT SUM DEFAULT '0'
) ENGINE=OLAP
AGGREGATE KEY(province, behavior_type, gender, age_group, minute_key)
DISTRIBUTED BY HASH(province) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD",
    "compaction_policy" = "time_series"
);

-- ========================
-- 3. 省级日汇总表 (Aggregate Key)
-- Hive 批量导入/MySQL 迁移的离线数据
-- 使用 RANGE 分区 + dynamic_partition 自动管理
-- ========================
CREATE TABLE IF NOT EXISTS dws_province_daily (
    province      VARCHAR(50) NOT NULL,
    date_key      DATE NOT NULL,
    pv            BIGINT SUM DEFAULT '0',
    uv            BIGINT REPLACE_IF_NOT_NULL DEFAULT '0',
    buy_cnt       BIGINT SUM DEFAULT '0'
) ENGINE=OLAP
AGGREGATE KEY(province, date_key)
PARTITION BY RANGE(date_key) ()
DISTRIBUTED BY HASH(province) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "3"
);

-- ========================
-- 4. 行为类型日汇总表 (Aggregate Key)
-- 替代 MySQL 的 demo2/demo3/demo5/demo7
-- ========================
CREATE TABLE IF NOT EXISTS dws_behavior_daily (
    behavior_type INT NOT NULL,
    gender        VARCHAR(10) NOT NULL,
    age           INT NOT NULL,
    date_key      DATE NOT NULL,
    cnt           BIGINT SUM DEFAULT '0'
) ENGINE=OLAP
AGGREGATE KEY(behavior_type, gender, age, date_key)
PARTITION BY RANGE(date_key) ()
DISTRIBUTED BY HASH(behavior_type) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "3"
);

-- ========================
-- 5. 用户画像宽表 (Duplicate Key)
-- 用于用户分析查询
-- ========================
CREATE TABLE IF NOT EXISTS ads_user_portrait (
    user_id        VARCHAR(50) NOT NULL,
    total_visits   BIGINT DEFAULT '0',
    total_traffic  BIGINT DEFAULT '0',
    avg_duration   DOUBLE DEFAULT '0.0',
    user_value     VARCHAR(20) COMMENT '高价值/中价值/普通',
    station_count  INT DEFAULT '0',
    category_diversity INT DEFAULT '0',
    interest_label VARCHAR(50),
    home_province  VARCHAR(50)
) ENGINE=OLAP
DUPLICATE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD"
);

-- ========================
-- 6. 用户最近行为 (Unique Key)
-- 记录每个用户最后一次行为时间，用于快速判断用户活跃状态
-- ========================
CREATE TABLE IF NOT EXISTS ads_user_lastest_behavior (
    user_id        VARCHAR(50) NOT NULL,
    last_event_time DATETIME NOT NULL,
    last_behavior_type INT NOT NULL COMMENT '1=浏览 2=收藏 3=加购 4=购买',
    last_province  VARCHAR(50),
    total_visits   BIGINT DEFAULT '0',
    total_buys     BIGINT DEFAULT '0',
    update_time    DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD",
    "compaction_policy" = "time_series"
);

-- 创建 doris_user 用户供 Spring Boot 使用
CREATE USER IF NOT EXISTS 'doris_user'@'%' IDENTIFIED BY 'doris123';
GRANT ALL ON ub_olap.* TO 'doris_user'@'%';

SHOW DATABASES;
USE ub_olap;
SHOW TABLES;
