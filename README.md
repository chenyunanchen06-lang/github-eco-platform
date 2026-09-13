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

安装剩余依赖：

```bash
D:/1/anaconda3/python.exe -m pip install pyspark==3.5.3 duckdb
```

**两个 Windows 上的坑（务必先看）**

1. **PySpark 必须锁 3.5.x**。本机是 Java 8，而 Spark 4.0 要求 Java 17，
   装最新版会直接起不来。
2. **需要设置 `HADOOP_HOME`** 指向 winutils 的上级目录，否则 PySpark 写入时会报
   `HADOOP_HOME and hadoop.home.dir are unset`：

   ```bash
   export HADOOP_HOME="D:/1/apache-hadoop-3.1.3-winutils-m/apache-hadoop-3.1.3-winutils-master"
   export PATH="$HADOOP_HOME/bin:$PATH"
   ```

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
scripts/download_gharchive.py   01  下载原始数据（并发 + 重试 + 断点续传）
scripts/raw_to_ods.py           02  gz -> ODS Parquet
docs/DESIGN.md                  表结构、脚本清单、里程碑、简历文案
data/                           数据落地（gitignore）
reports/                        数据质量报告（gitignore）
```

## 进度

- [x] 环境勘察、数据源验证、工程骨架
- [x] **第 1 周 · 采集打通**（3 小时样本已端到端跑通，脚本可用）
  - [ ] 扩大到 7 天全量
- [ ] 第 2 周 · 清洗聚合
- [ ] 第 3 周 · 看板与校验
- [ ] 第 4 周 · 智能问数
