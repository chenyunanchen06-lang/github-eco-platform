# GitHub 开源生态数据分析与智能查询平台

面向 **数据开发** 与 **AI 应用开发** 双方向的个人实战项目：把 GitHub 全站公开事件
从原始采集、数仓分层建模，一路做到可视化看板与自然语言问数。

完整设计（表结构、脚本清单、里程碑、简历文案）见 **[docs/DESIGN.md](docs/DESIGN.md)**。

---

## 数据源

[GitHub Archive](https://www.gharchive.org/) —— 每小时一个文件，包含 GitHub 上全部公开事件
（Push / PullRequest / Issues / Watch / Fork / Release …）。

- 地址规则：`https://data.gharchive.org/{YYYY-MM-DD}-{H}.json.gz`
- 公开免费、无需 API Key、无爬虫对抗与合规风险

**实测规模（2026-09-12 的 0/1/2 时，共 3 个文件）**

| 指标 | 实测值 |
|---|---|
| 单文件体积 | 60.8 / 56.8 / 48.2 MB（压缩），均值约 55 MB |
| 单文件行数 | 约 6.7 万条事件/小时 |
| ODS 单条体积 | 817 字节（含 payload） |
| **推算单日** | **约 160 万条事件 / 约 1.3 GB（压缩）** |
| **推算单周** | **约 1100 万条事件 / 约 9 GB（压缩）** |

> ⚠️ **文件体积波动极大**：对比测过的 `2026-09-01-0` 只有 6.9 MB（5.2 万条）。
> 不同日期/时段差近 9 倍。**不要照抄任何人的规模数字，按自己实际的日期范围实测。**
> 建议先跑 1 天确认磁盘占用，再决定拉多少天。
>
> ⚠️ **磁盘预留**：单周约 9 GB 原始数据 + 约 9 GB ODS Parquet。

## 三个必须知道的坑

1. **小时位不补零** —— 正确是 `2026-09-01-0.json.gz`，写成 `-00.json.gz` 直接 404。
2. **`HEAD` 请求返回 403** —— 必须用 `GET`。
3. **高频访问会被限流（间歇性 403）** —— 脚本已内置全局限速器（默认相邻请求间隔 0.8 秒）
   与指数退避重试。不要为了"跑快点"把 `--sleep` 调到 0。

## 分层架构

```
data/raw       原始 gz（dt=YYYY-MM-DD/HH.json.gz）
  └─ data/ods   ODS：顶层字段展开 + payload 原样保留（Parquet / zstd）
       └─ data/dwd   DWD：事件事实表 + 仓库/用户/日期维度表
            └─ data/ads   ADS：6 张面向分析的应用宽表
                 └─ warehouse.duckdb → Streamlit 看板 + Text-to-SQL Agent
```

## 环境

本机实测（**推荐直接用 Anaconda 环境** `D:\1\anaconda3\python.exe`）：

| 依赖 | 状态 |
|---|---|
| pandas 2.3.3 / pyarrow 16.1.0 / requests 2.34.2 | 已装 —— 第 1 周开箱即跑 |
| streamlit 1.37.1 | 已装 —— 第 3 周可用 |
| Java | 1.8.0_261 |
| winutils | `D:\1\apache-hadoop-3.1.3-winutils-m\apache-hadoop-3.1.3-winutils-master\bin\winutils.exe` |
| pyspark / duckdb | **待安装** |

### 两个环境（重要）

Spark 脚本必须跑在**独立的 Python 3.11 环境**里，其余脚本用 Anaconda base 即可：

| 用途 | 解释器 |
|---|---|
| 采集 / ODS / DuckDB / 看板 | `D:\1\anaconda3\python.exe`（3.12） |
| **PySpark 脚本** | **`D:\1\anaconda3\envs\spark\python.exe`（3.11.16）** |

环境创建方式（已建好，无需重跑）：

```bash
D:\1\anaconda3\Scripts\conda.exe create -n spark python=3.11 -y \
  -c https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main/ --override-channels
D:\1\anaconda3\envs\spark\python.exe -m pip install \
  -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark==3.5.3
```

### 三个必须知道的坑（全部踩过）

1. **PySpark 3.5.x 不能用 Python 3.12**。元数据写的是 `Requires-Python: >=3.8`，
   但实测在 Anaconda 的 3.12.7 下 worker 会**静默死亡**，只留下
   `Python worker exited unexpectedly (crashed)` + `java.io.EOFException`。
   曾逐一排除 PYTHONPATH 污染、winutils 路径、`spark.driver.host`、IPv6 优先、
   `worker.reuse`、PATH 顺序等全部无效，**换 3.11.16 立刻正常**。
   `spark_env.py` 已内置版本检查，会直接拦住而不是让你对着 EOFException 抓瞎。
2. **PySpark 必须锁 3.5.x**。本机 Java 是 1.8，Spark 4.0 要求 Java 17。
3. **`-Dhadoop.home.dir` 必须用正斜杠**。传 Windows 反斜杠路径时反斜杠会被
   逐层吞掉，变成 `D:1hadoop-3.1.3`，Spark 报 "is not an absolute path"。
   `spark_env.py` 已用 `Path.as_posix()` 处理。

`HADOOP_HOME` / `PYSPARK_PYTHON` / `SPARK_LOCAL_IP` 等都由 `spark_env.py` 统一设置，
Spark 脚本里只需 `from spark_env import get_spark`，不用手工配环境。

## 快速开始

```bash
PY="D:/1/anaconda3/python.exe"

# 先小样验证（1 天里的 3 个小时，约 166 MB，约 45 秒）
$PY scripts/download_gharchive.py --days 1 --hours 0,1,2
$PY scripts/raw_to_ods.py

# 确认无误后再拉全量（1 天 = 24 个文件，约 1.3 GB）
$PY scripts/download_gharchive.py --days 1
$PY scripts/raw_to_ods.py

# 拉一周（约 9 GB，务必先确认磁盘）
$PY scripts/download_gharchive.py --days 7
$PY scripts/raw_to_ods.py
```

**已验证可跑通**（3 个小时的真实数据）：

```
事件总行数   199,433     event_id 唯一率 100%，空值 0
原始 gz      165.8 MB
ODS Parquet  155.3 MB    （单条 817 字节）
耗时         16.6s
```

## 目录结构

```
config.py                       全局配置（路径 / 日期范围 / 并发参数）
spark_env.py                    PySpark 会话工厂（Windows 环境适配 + 版本检查）
scripts/download_gharchive.py   01  下载原始数据（并发 + 限速 + 断点续传）
scripts/raw_to_ods.py           02  gz -> ODS Parquet
scripts/check_ods.py            02b ODS 验收报告
scripts/ods_to_dwd.py           03  ODS -> DWD（PySpark，按天增量）
scripts/dwd_to_ads.py           04  DWD -> ADS 6 张应用宽表
scripts/dq_check.py             05  5 条数据质量校验 -> reports/dq_report.json
scripts/build_duckdb.py         06  挂载 DuckDB 视图 -> warehouse.duckdb
app/dashboard.py                07  Streamlit 可视化看板
app/pages/2_text2sql.py         09  智能问数界面
text2sql/agent.py               08  Text-to-SQL Agent（含安全护栏与模板兜底）
docs/DESIGN.md                  表结构、脚本清单、里程碑、简历文案
```

## 智能问数（Text-to-SQL）

用中文提问，自动生成 SQL 并查询（`app/pages/2_text2sql.py`）。

**不配 API Key 也能跑** —— 会退化为模板模式，用预置 SQL 回答常见问题；
配了 Key 就走 LLM，支持任何 OpenAI 兼容接口：

```bash
export TEXT2SQL_API_KEY=sk-xxx
export TEXT2SQL_BASE_URL=https://api.deepseek.com/v1   # 可换成通义/智谱/Ollama
export TEXT2SQL_MODEL=deepseek-chat
```

**三重保护**

1. **只读护栏**：只允许 `SELECT` / `WITH`，出现 `DROP`/`DELETE`/`COPY`/`INSTALL`
   等任何写操作关键字直接拒绝（`text2sql/agent.py::is_safe`）
2. **执行反馈重试**：SQL 报错时把数据库的**原始错误信息**回灌给模型重新生成，
   最多 3 轮。模型第一次常写错列名，看到
   `Binder Error: Referenced column "xxx" not found` 之后基本能自己改对
3. **拒答而非编造**：3 轮仍失败就明确报错，不返回任何数据

> ⚠️ **别用本地 4B 模型做 Text-to-SQL**。SQL 生成要求精确记忆列名，
> 小模型会编造不存在的字段。用 API 成本极低（每次几百 token）。

## 进度

- [x] 环境勘察、数据源验证、工程骨架
- [x] 第 1 周 · 采集打通（**7 天 168 个文件 / 6.19 GB**）
- [x] 第 2 周 · 清洗聚合（**DWD 1013 万行 + ADS 6 表 428 万行**，事实表构建提速 4.9x）
- [x] 第 3 周 · 看板与校验（DuckDB 11 视图 + Streamlit 看板 + 5 条质量校验）
- [x] 第 4 周 · 智能问数（Text-to-SQL Agent + 安全护栏 + 模板兜底）
- [ ] 部署上线 + 补齐缺失的 27 小时数据
