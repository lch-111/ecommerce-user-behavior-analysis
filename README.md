# 电商用户行为流批一体分析平台

基于 Hadoop/Hive 离线数仓改造的 **Lambda 批流一体架构**。实现电商用户行为数据的双路径处理：
**离线批处理（Hive → MySQL）** 与 **实时流处理（Kafka → Doris）** 通过统一服务层合并展示，在大屏上直观对比两类数据的差异与一致性。

---

## 架构总览：Lambda 批流一体

```
┌──────────────────────────────────────────────────────────────────────────┐
│                             数据源                                       │
│                shuju.txt — 318万条电商用户行为日志                        │
│          (user_id, age, gender, item_id, behavior_type, province)        │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
     ┌──────────────▼──────────────┐  ┌───────▼──────────────────┐
     │       BATCH LAYER           │  │     SPEED LAYER          │
     │      (离线批处理层)           │  │     (实时流处理层)         │
     │                             │  │                          │
     │  Flume → HDFS → Hive ETL   │  │  Python 模拟器            │
     │  (ODS → DWD → ADS)         │  │  500条/秒                 │
     │                             │  │                          │
     │  4节点 Hadoop 集群           │  │  Kafka 消息队列           │
     │                             │  │                          │
     │  Sqoop → MySQL (VM)         │  │  Python 分钟窗口聚合      │
     │  T+1 数据                    │  │  → Doris Stream Load    │
     └──────────────┬──────────────┘  └────────────┬──────────────┘
                    │                              │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼────────────────┐
                    │      SERVING LAYER             │
                    │      (统一服务层)               │
                    │                               │
                    │  Spring Boot (port 8111)       │
                    │  ├─ MySQL 数据源 (getPartX)    │
                    │  └─ Doris 数据源 (getDorisX)   │
                    │                               │
                    │  可视化大屏 (ECharts)           │
                    │  ├─ 离线源: 全量历史数据        │
                    │  ├─ 实时源: 最近5分钟聚合       │
                    │  └─ 批流对比: 差异率校验        │
                    └───────────────────────────────┘
```

---

## 批流一体核心流程

### 离线批处理链路

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ shuju.txt│───→│  HDFS    │───→│ Hive ODS │───→│Hive DW D │───→│Hive ADS  │
│ 318万行   │    │ 存储     │    │ 原始层    │    │ 明细层    │    │ 应用层    │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └─────┬────┘
                                                                      │
                                                                      ▼
                                                              ┌──────────────┐
                                                              │   Sqoop      │
                                                              │  Hive→MySQL  │
                                                              └──────┬───────┘
                                                                     │
                                                              ┌──────▼───────┐
                                                              │  MySQL (VM)  │
                                                              │  demo1~demo7 │
                                                              │  全量历史数据  │
                                                              └──────────────┘
```

**数据流向**:
1. 原始日志上传到 HDFS
2. Hive ODS 层加载原始数据 (`ods_user_data`: id, user_id, age, gender, item_id, behavior_type, item_category, time, province)
3. DWD 层按维度拆分为 7 张明细表 (province, behavior_type, date, user_id, age, gender)
4. DWS/ADS 层聚合 → Sqoop 导出到 MySQL
5. Spring Boot 读取 MySQL 提供离线 API

### 实时流处理链路

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────────┐
│ shuju.txt│───→│  Python  │───→│  Kafka   │───→│ Python 消费者     │
│ 318万行   │    │ 模拟器   │    │ tele_log │    │ 分钟窗口聚合      │
└──────────┘    │500条/秒  │    └──────────┘    └────────┬─────────┘
                └──────────┘                              │
                                                   ┌──────▼─────────┐
                                                   │ Doris Stream   │
                                                   │ Load (重试3次)  │
                                                   └──────┬─────────┘
                                                          │
                                                   ┌──────▼─────────┐
                                                   │  Doris OLAP    │
                                                   │  实时聚合表     │
                                                   │  最近5分钟数据  │
                                                   └────────────────┘
```

**数据流向**:
1. Python 模拟器从 shuju.txt 逐行读取，500条/秒发送到 Kafka
2. Python 消费者实时消费 Kafka，按 `(province, behavior_type, gender, age_group)` 维度做分钟窗口聚合
3. 每分钟通过 Stream Load 写入 Doris 的 `dws_realtime_minute` 表（指数退避重试 3 次）
4. Spring Boot 读取 Doris 提供实时 API

### 批流融合展示

| 维度 | 离线 (MySQL) | 实时 (Doris) | 差异 |
|------|-------------|--------------|------|
| 数据范围 | 全量 318万条 | 最近 5 分钟 | 数量级差异 |
| 更新频率 | T+1 | 每秒 | 实时可见 |
| 查询接口 | `getPart1/2/3/5/7` | `getDorisPart1` | 路由不同 |
| 大屏来源 | 离线总览/地图/趋势等 | 实时GMV/PV/UV | 左右分区 |

大屏底部状态栏实时显示**差异率**（模拟），超过阈值触发告警，体现批流数据一致性校验。

---

## 实时/离线切换机制

```java
// AppService.java — 数据源路由
public List<Map<String, Object>> getDorisProvince() {
    // 优先查 Doris 实时分钟聚合表
    List<Map<String, Object>> rt = dorisDao.getRealtimeMinuteProvince();
    if(rt != null && !rt.isEmpty()) return rt;
    // 降级到离线数据
    return getPart1();
}
```

```javascript
// 前端源切换 (index.html)
// MySQL 离线 → 调 getPart1 (全量历史)
// Doris 实时 → 调 getDorisPart1 (最近5分钟聚合)
// 大屏数据随源切换实时变化
```

---

## Doris 表模型设计

依据生产最佳实践选择模型:

| 模型 | 适用场景 | 当前表 |
|------|---------|--------|
| **Aggregate Key** | 预聚合指标 (PV, UV, 计数) | `dws_realtime_minute`, `dws_province_daily`, `dws_behavior_daily` |
| **Unique Key** | 实时更新最新状态 | `ads_user_lastest_behavior` (用户最近行为) |
| **Duplicate Key** | 全量保留明细 | `dwd_user_behavior_realtime` |

**Compaction 优化**: 全部 Aggregate/Unique 表设置 `compaction_policy = 'time_series'`，针对实时高频导入场景优化版本合并策略，避免 Compaction 堆积。

---

## 大屏可视化

| 分区 | 内容 | 数据源 | 刷新 |
|------|------|--------|------|
| 左列 | 实时GMV/PV/UV、离线总览、告警、实时转化 | 混合 | 3s |
| 中列 | 中国地图 (省份访问)、状态指标卡 | MySQL | 60s |
| 右列 | 热销Top10、访问趋势、词云 | MySQL | 60s |
| 底部 | 性别/漏斗/年龄/省份柱状图 | MySQL | 60s |

---

## 部署指南

### 组件启动

```bash
# 1. Doris (Docker)
docker compose -f code/deploy/docker-compose-doris.yml up -d
docker exec -i doris mysql -h127.0.0.1 -P9030 -uroot < code/deploy/init-doris.sql

# 2. Kafka (Docker)
docker compose -f code/deploy/docker-compose-kafka.yml up -d

# 3. 加载离线数据到 Doris
py scripts/bootstrap_doris.py

# 4. 启动实时模拟器（可选）
py scripts/realtime_simulator.py

# 5. Spring Boot (Docker)
docker run -d --name telecom-app --network deploy_default -p 8111:8111 -e DORIS_HOST=doris telecom-app:latest
```

访问 **http://localhost:8111**

---

## 项目结构

```
ecommerce-user-behavior-analysis/
├── README.md
├── code/
│   ├── shuju.txt               # 原始数据 318万行
│   ├── hive分析.txt             # Hive ETL 全流程 SQL
│   ├── browse/                 # Spring Boot + 前端
│   │   ├── pom.xml
│   │   └── src/main/
│   │       ├── java/com/browse/
│   │       │   ├── Application.java         # 启动入口
│   │       │   ├── AppController.java       # REST API
│   │       │   ├── AppService.java          # 业务 + Doris路由
│   │       │   ├── AppDao.java              # MySQL Mapper
│   │       │   ├── DorisDao.java            # Doris 独立连接
│   │       │   └── KafkaPro.java            # Kafka 生产者
│   │       └── resources/
│   │           ├── application.properties   # 数据源配置
│   │           └── static/                  # 前端
│   │               ├── index.html           # 主大屏
│   │               └── js/ (echarts, jquery, china.js)
│   └── deploy/                  # Docker 部署
│       ├── docker-compose-doris.yml
│       ├── docker-compose-kafka.yml
│       ├── init-doris.sql
│       └── Dockerfile
├── scripts/                    # Python 工具
│   ├── bootstrap_doris.py      # MySQL → Doris 批量加载
│   └── realtime_simulator.py   # 实时模拟器 500条/秒
└── flink-realtime/             # Flink (可选)
    └── pom.xml
```

---

## 项目·亮点

- **Lambda 批流一体架构**: Hive 离线批处理 + Kafka 实时流处理 → Doris 统一 OLAP 服务层 → ECharts 大屏展示
- **Doris 优化**: 三种模型选用 (Aggregate/Unique/Duplicate)，`time_series` Compaction 策略，动态分区
- **实时模拟器**: Python 多线程 500条/秒 Kafka 生产 + Stream Load 指数退避重试
- **双源大屏**: 10+ ECharts 图表，实时/离线源一键切换，数据质量校验

---

## License

MIT
