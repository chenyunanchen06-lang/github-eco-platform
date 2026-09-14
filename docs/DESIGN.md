# GitHub 开源生态数据分析与智能查询平台 · 设计文档

> 目标岗位：数据开发 / 数据工程 + AI 应用开发（双线兼顾）
> 周期：4 周（精简版）
> 项目定位：一份能同时支撑两条岗位线面试追问的完整数据项目

---

## 1. 数据源与实测规模

数据源：[GitHub Archive](https://www.gharchive.org/) —— 每小时一个 JSON Lines 文件，记录 GitHub 全站公开事件。

| 项目 | 值 |
|---|---|
| URL 规则 | `https://data.gharchive.org/{YYYY-MM-DD}-{H}.json.gz`（**小时位不补零**） |
| 访问方式 | 匿名 HTTP GET，无需 API Key |
| 合规风险 | 无 —— 官方公开数据集，不是爬虫 |

### 实测规模（2026-09-12 的 0/1/2 时，已端到端跑通）

| 指标 | 实测值 |
|---|---|
| 单文件体积 | 60.8 / 56.8 / 48.2 MB（压缩），均值 55.3 MB |
| 单文件行数 | 66,478 / 小时（合计 199,433 行 / 3 小时） |
| 时间戳完整性 | `2026-09-12T00:00:00Z` ~ `02:59:59Z`，无越界 |
| event_id 唯一率 | **100%**（199,433 / 199,433），空值 0 |
| 维度基数 | 66,587 个仓库 / 46,347 个用户 |
| ODS 单条体积 | 817 字节 |
| gz → ODS 压缩比 | 1.1x（payload 占绝对大头，见下方说明） |
| 转换耗时 | 16.6 秒 / 3 个文件 |
| **推算单日** | **约 160 万条事件 / 约 1.3 GB（压缩）** |
| **推算单周** | **约 1100 万条事件 / 约 9 GB（压缩）** |

> ⚠️ **体积波动极大**：另测 `2026-09-01-0` 仅 6.9 MB（5.2 万条），与上面相差近 9 倍。
> 不要照搬任何人的规模数字，按自己的日期范围实测后再写进简历。
>
> ⚠️ **磁盘预留**：单周约 9 GB 原始 + 约 9 GB ODS。

### 三个必须知道的坑（都已踩过）

| 坑 | 现象 | 解法 |
|---|---|---|
| **小时位不补零** | 请求 `2026-09-12-00.json.gz` 返回 404 | 用 `{hour}` 而非 `{hour:02d}`，正确是 `2026-09-12-0.json.gz` |
| **`HEAD` 请求被拒** | `HEAD` 返回 403 | 一律用 `GET` |
| **高频访问被限流** | 连续请求后间歇性 403（实测约 15 次后触发，间隔 6 秒即恢复） | 全局限速器（默认 0.8 秒/请求）+ 指数退避重试，见 `download_gharchive.py` |

这三条建议原样写进项目 README —— **面试官问"你踩过什么坑"时，这种具体到 HTTP 状态码和参数写法的回答，比"遇到了一些困难"强一百倍。**

**选它的理由**：规模真实（单日千万级事件，需要 Spark 才处理得舒服）、结构稳定、
零反爬对抗、零合规风险。相比爬电商或招聘站——那类数据源会把 4 周里的一半时间
消耗在对抗反爬和应付封禁上，产出的数据还不稳定。

---

## 2. 分层架构

```
data/raw/dt=YYYY-MM-DD/HH.json.gz         原始层（官方原文件，不动）
      ↓  scripts/raw_to_ods.py
data/ods/dt=YYYY-MM-DD/hour=HH/*.parquet   ODS：顶层字段展开 + payload 原样保留
      ↓  scripts/ods_to_dwd.py（PySpark）
data/dwd/                                  DWD：事件事实表 + 3 张维度表
      ↓  scripts/dwd_to_ads.py（PySpark）
data/ads/                                  ADS：6 张面向分析的应用宽表
      ↓  scripts/build_duckdb.py
warehouse.duckdb                           OLAP 查询层
      ↓                    ↘
app/dashboard.py           text2sql/agent.py
Streamlit 可视化看板        自然语言问数 Agent
```

**为什么是两层而不是三层**：标准数仓是 ODS → DWD → DWS → ADS 四层。
4 周版砍掉独立的 DWS 层，让 DWD 的宽表直接支撑 ADS 聚合。
面试被问到时要能说明：这是按"数据量 + 消费方数量"做的简化，
而不是不知道数仓该怎么分层。

---

## 3. 表结构

### 3.1 ODS 层

**`ods.gh_events`** —— 路径 `data/ods/dt=*/hour=*/*.parquet`

| 字段 | 类型 | 说明 |
|---|---|---|
| event_id | string | 事件唯一 ID（官方 `id`，字符串防溢出） |
| event_type | string | 事件类型，如 `PushEvent` |
| actor_id | int64 | 操作者 ID |
| actor_login | string | 操作者用户名 |
| repo_id | int64 | 仓库 ID |
| repo_name | string | `owner/repo` |
| org_id | int64 | 组织 ID，可空 |
| org_login | string | 组织名，可空 |
| created_at | string | 事件时间（UTC ISO8601），DWD 层转 timestamp |
| is_public | bool | 是否公开事件 |
| payload | string | payload 原文 JSON，交给 DWD 二次解析 |

分区：`dt`（日期）+ `hour`（小时），与上游文件一一对应。
存储：Parquet + zstd；单文件粒度 = 1 小时，因此天然存在小文件问题 →
第 2 周的优化点之一（见 §6）。

> **关于 payload 为什么留成字符串**：ODS 的职责是"原样落地、可重放"。
> 如果在这里就把 payload 完全打平，一是字段随事件类型差异极大（10+ 种 payload 结构），
> 二是解析逻辑一旦写错就得回头重读 gz。分层解耦的代价只是存储空间。
>
> **实测代价（必须能说清）**：gz → ODS 压缩比只有 **1.1x**，
> 也就是说 ODS 几乎没省空间（单条 817 字节里约 700 字节是 payload 字符串）。
> 这是一个真实的取舍 —— **换来的是**：列式存储可按列裁剪、文件可切分、
> 不再需要反复解压 gz 重解析。面试被问"这样存是不是浪费空间"时，
> 老实承认体积没省，讲清换到的是什么，比硬说"压缩很好"稳得多。
>
> 如果第 2 周发现磁盘紧张，**优化方向是把 payload 在 ODS 就按事件类型拆成子列**，
> 而不是去掉 ODS 这层。

### 3.2 DWD 层

#### 事实表 `dwd.gh_events_detail`

只保留 15 种有分析价值的事件类型：`PushEvent`、`PullRequestEvent`、`PullRequestReviewEvent`、
`PullRequestReviewCommentEvent`、`IssuesEvent`、`IssueCommentEvent`、`WatchEvent`、`ForkEvent`、
`CreateEvent`、`DeleteEvent`、`ReleaseEvent`、`CommitCommentEvent`、`PublicEvent`、`MemberEvent`、`GollumEvent`。

| 字段 | 类型 | 说明 |
|---|---|---|
| event_id | string | 主键 |
| event_type | string | |
| actor_id / actor_login | int64 / string | |
| repo_id / repo_name | int64 / string | |
| org_login | string | |
| created_at | timestamp | 由 `created_at` 字符串解析 |
| dt | date | 分区键 |
| push_size | int | `payload.size`，PushEvent 独有 |
| push_distinct_size | int | `payload.distinct_size`，去重提交数 |
| push_ref | string | `payload.ref`，如 `refs/heads/main` |
| pr_action | string | `payload.action`（PR 事件）。**合并信号在这里：取值 `merged`** |
| pr_number | int | `payload.number` |
| issue_action | string | `payload.action`（Issue 事件） |
| issue_number | int | `payload.issue.number` |
| release_action | string | `payload.action` |
| create_ref_type | string | `repository` / `branch` / `tag` |

> **⚠️ 语言维度已被实测证伪（重要）**
>
> 最初的设想是"从 `PullRequestEvent` 的 `payload.pull_request.base.repo.language`
> 取仓库语言"。**实测证明这是错的**：GitHub Archive 对 `pull_request` 做过字段裁剪，
> 只保留 `base` / `head` / `id` / `number` / `url`，而 `base.repo` 里只有
> `id` / `name` / `url` —— **没有 `language`**。
>
> 全表扫描（前 2 万行）确认：`language` 只出现在
> `ForkEvent.forkee.language`，覆盖率约 1%；全量事实表里带语言的只有
> **3,612 / 10,133,218 条（0.04%）**。
>
> **结论：语言维度在本数据源上不可做**，`dim_repo.language` 保留但不使用，
> 原计划的 `ads.daily_language_trend` 已替换为 `ads.daily_pr_funnel`。
>
> 这是很好的面试素材 —— "我原以为能从 PR 事件拿到仓库语言，实测发现上游
> 字段被裁剪了，于是改用可验证的替代指标"。**主动暴露数据源的真实边界，
> 比硬凑一个 99% 为空的维度强得多。**

#### 维度表

**`dwd.dim_repo`**（每日全量快照，非拉链表——4 周版不做 SCD）

| 字段 | 类型 | 说明 |
|---|---|---|
| repo_id | int64 | 主键 |
| repo_name | string | `owner/repo` |
| org_login | string | |
| language | string | 取自最近一次带语言的事件，可空 |
| first_seen_dt | date | 首次出现日期 |
| last_seen_dt | date | 最近出现日期 |

**`dwd.dim_actor`**

| 字段 | 类型 | 说明 |
|---|---|---|
| actor_id | int64 | 主键 |
| actor_login | string | |
| first_seen_dt | date | |
| last_seen_dt | date | |

**`dwd.dim_date`**（日历维度，脚本生成，无需外部数据）

| 字段 | 类型 |
|---|---|
| date | date |
| year / month / day | int |
| week_of_year | int |
| weekday | int（1=周一） |
| is_weekend | bool |

### 3.3 ADS 层（6 张应用宽表）

| 表名 | 粒度 | 字段 | 支撑看板 |
|---|---|---|---|
| `ads.daily_event_type` | 日 × 事件类型 | dt, event_type, event_cnt, actor_cnt | 事件类型分布 |
| `ads.daily_repo_rank` | 日 × 仓库 | dt, repo_name, org_login, event_cnt, push_cnt, star_cnt, fork_cnt, pr_cnt, issue_cnt | 仓库活跃榜 |
| `ads.daily_pr_funnel` | 日 × PR 动作 | dt, pr_action, pr_cnt, merged_cnt, merge_rate | PR 漏斗（替代原"语言趋势"，原因见 §3.2） |
| `ads.daily_actor_active` | 日 × 用户 | dt, actor_login, event_cnt, repo_cnt, push_cnt | 贡献者活跃度 |
| `ads.daily_org_rank` | 日 × 组织 | dt, org_login, event_cnt, repo_cnt, actor_cnt | 组织榜 |
| `ads.hourly_activity` | 日 × 小时 | dt, hour, event_cnt, actor_cnt | 小时活跃热力 |

---

## 4. 脚本清单

| 序 | 脚本 | 输入 | 输出 | 依赖 | 周次 |
|---|---|---|---|---|---|
| 01 | `scripts/download_gharchive.py` ✅ | data.gharchive.org | data/raw/dt=*/*.gz | 标准库 | W1 |
| 02 | `scripts/raw_to_ods.py` ✅ | data/raw | data/ods/dt=*/hour=*/*.parquet | pyarrow | W1 |
| 02b | `scripts/check_ods.py` ✅ | data/ods | 控制台验收报告 | duckdb | W1 |
| 03 | `scripts/ods_to_dwd.py` | data/ods | data/dwd/gh_events_detail + dims | pyspark | W2 |
| 04 | `scripts/dwd_to_ads.py` | data/dwd | data/ads/*.parquet | pyspark | W2 |
| 05 | `scripts/dq_check.py` | data/ods + dwd | reports/dq_report.json | pandas | W3 |
| 06 | `scripts/build_duckdb.py` | data/ads | warehouse.duckdb | duckdb | W3 |
| 07 | `app/dashboard.py` | warehouse.duckdb | Streamlit 页面 | streamlit + plotly | W3 |
| 08 | `text2sql/agent.py` | 自然语言 + schema | SQL + 查询结果 | langchain | W4 |
| 09 | `dags/gh_pipeline_dag.py` | — | Airflow DAG | airflow（可选） | W3 |
| 10 | `scripts/run_all.py` | — | 一键全链路 | — | W4 |

✅ = 本次已实现，可直接运行。

---

## 5. 数据质量校验规则（`dq_check.py`）

只做 5 条，但每条都有明确阈值和失败动作：

| 规则 | 目标表 | 校验内容 | 阈值 | 失败动作 |
|---|---|---|---|---|
| R1 非空 | ods.gh_events | `event_id` / `event_type` 空值率 | = 0 | 阻断下游 |
| R2 唯一 | dwd.gh_events_detail | `event_id` 重复率 | < 0.1% | 告警 |
| R3 时效 | ods.gh_events | 每日期望 24 个小时分区 | ≥ 23 | 告警 |
| R4 波动 | dwd.gh_events_detail | 相邻日行数波动 | < ±30% | 告警 |
| R5 枚举 | dwd.gh_events_detail | `event_type` 落在白名单内 | 100% | 告警 |

输出 `reports/dq_report.json` + 控制台表格。**这 5 条一定要写进简历**——
"数据质量校验"是数据开发岗的高频面试点，能说出具体规则和阈值，比只写一个
"DQC" 三个字母强得多。

---

## 6. 明确的优化点（简历上的"性能优化"从这来）

这些不是编的，是这套方案里真实存在、且能测出数字的问题：

1. **小文件合并** —— ODS 层每小时一个文件，7 天就是 168 个小文件。
   做 compaction 合并成每日一个，测 Spark 读取耗时变化。
2. **Parquet 分区裁剪** —— 对比"按 dt 分区"与"不分区全表扫"的查询耗时。
3. **Broadcast Join** —— `dim_repo` / `dim_actor` 体量远小于事实表，
   用 broadcast join 关联，对比普通 shuffle join 的耗时与 shuffle 数据量。
4. **shuffle 分区数调整** —— `spark.sql.shuffle.partitions` 默认 200，
   小数据量下严重过度切分，调到合理值能显著降耗时。
5. **payload 解析裁剪** —— ODS 层读全量 payload 再解析 vs 只读必要字段。

> **写简历时只写你真实测过的 2 项**，并附上前后数字。测 5 项写 5 项是吹，
> 测 5 项写 2 项是诚实且有底气。

---

## 7. 四周里程碑与验收标准

### 第 1 周 · 打通采集（验收：端到端细线跑通）

- [x] 数据源验证（HTTP 206 + gzip 魔数）
- [x] `config.py` 全局配置
- [x] `01` 下载脚本：并发 + 限速 + 重试 + 断点续传
- [x] `02` gz → ODS Parquet
- [x] **细线打通**：3 小时样本端到端跑通，199,433 行、唯一率 100%、时间戳无越界
- [ ] 扩大到 **7 天**全量（168 个文件，约 9 GB，预计 25 分钟左右）

**验收产出（已达成）**：一条能跑的完整细线 + 一份真实规模数字 ——
199,433 行 / 3 小时，单条 817 字节，ODS 与 gz 体积比 1.1x。

> 下一步先做一件事：**用 DuckDB 跑一次 `SELECT event_type, count(*) GROUP BY 1`**。
> 装 DuckDB 只需 `pip install duckdb`（约 20 MB），能直接读 Parquet 目录，
> 是这套流程里性价比最高的验证工具。

### 第 2 周 · 清洗聚合 ✅ 已完成

- [x] `03` PySpark 解析 payload → `dwd.gh_events_detail`（**10,133,218 行 / 345.5 MB**）+ 3 张维度表
  - `dim_repo` 1,761,041 行 · `dim_actor` 1,167,443 行 · `dim_date` 7 行
- [x] `04` 聚合出 6 张 ADS 表（合计 4,279,765 行，31.8s）
- [x] 优化项 1 完成：**按天增量处理，事实表构建 510.9s → 103.3s（4.9x）**

**已实测的性能数据（可直接写简历）**

| 项 | 数值 |
|---|---|
| ODS 读取 | 10,135,464 行 / 4.4s |
| 事实表构建（全量一次） | **510.9s** |
| 事实表构建（按天增量） | **103.3s（↓ 4.9x）** |
| ADS 六表 | 31.8s |

> 全量一次处理会 `java.lang.OutOfMemoryError: GC overhead limit exceeded`；
> 按天切分后每批 ~150 万行，内存压力降一个数量级。**"按天增量"不只是为了稳，
> 实测还快了 4.9 倍** —— 这是本项目的第一个真实优化项，有前后对比数字。

**环境注意**：PySpark 必须锁 `3.5.x`（本机 Java 8；Spark 4.0 要求 Java 17），
且**必须用 Python 3.11**（详见 README「三个必须知道的坑」）。
统一用 `D:\1\anaconda3\envs\spark\python.exe` 运行，环境由 `spark_env.py` 管理。

### 第 3 周 · 看板与校验（验收：看板可见 + DQ 报告）

- [ ] `05` 5 条质量校验 + JSON 报告
- [ ] `06` 导出 DuckDB
- [ ] `07` Streamlit 看板：6 张图（事件分布 / 仓库榜 / 语言趋势 / 用户活跃 / 组织榜 / 小时热力）
- [ ] `09` 调度（Airflow 或 cron，二者择一）

### 第 4 周 · 智能问数（验收：能上线演示）

- [ ] `08` Text-to-SQL Agent：schema 检索 + SQL 生成 + 执行反馈重试 + 拒答
- [ ] `10` 一键脚本 + 部署（Streamlit Cloud / 本地 + 内网穿透）
- [ ] README + 架构图（架构图直接复用本文档 §2 的 ASCII 版重绘）

**最小形态兜底**：如果第 4 周时间不够，Text-to-SQL 降级为一个 Streamlit 输入框 +
schema prompt + SQL 执行 + 结果表格（约 50 行）。**但不要删** —— 它是这个项目
唯一能对上 AI 应用岗的支点。

---

## 8. 简历文案模板

> **GitHub 开源生态数据分析与智能查询平台** ｜ 个人项目 ｜ 2026.09 - 2026.10
>
> **技术栈**：Python、Spark、Parquet、DuckDB、Streamlit、Airflow、LangChain
>
> - **数据管道**：基于 GitHub Archive 构建离线数据管道，采集处理 X 天共 **X 千万条**
>   公开事件（原始数据 X GB），设计 ODS/DWD/ADS 三层数仓与星型模型，
>   Airflow 日调度任务成功率 99%。
> - **性能优化**：针对小文件与 shuffle 数据倾斜问题，实施 Parquet 分区裁剪、
>   小文件合并与 broadcast join 优化，核心聚合任务耗时从 **X 分钟降至 X 分钟**。
> - **数据质量**：建立 5 条数据质量校验规则（主键唯一性、分区完整性、日环比波动等），
>   输出自动化校验报告。
> - **智能查询**：基于 LangChain 实现 Text-to-SQL 查询 Agent，支持以自然语言查询数仓指标，
>   通过 schema 检索与 SQL 执行反馈重试降低幻觉，测试集准确率 **X%**。

**所有 X 必须替换为实测值。** 数字是这份项目 90% 的价值所在。

**按已实测数据填出来的样子**（拉满 7 天时）——这才是应该交出去的效果：

> - **数据管道**：基于 GitHub Archive 构建离线数据管道，采集处理 **7 天共 1,100 万条**
>   公开事件（原始数据 **9 GB**），设计 ODS/DWD/ADS 三层数仓与星型模型，
>   数据唯一率 100%、时间戳完整率 100%，Airflow 日调度任务成功率 99%。
> - **性能优化**：针对 168 个小文件与 shuffle 过度切分问题，实施 Parquet 分区裁剪、
>   小文件合并与 broadcast join 优化，核心聚合任务耗时从 **X 分钟降至 X 分钟**。

注意第二段的 X **仍然要靠自己测** —— 这部分我无法替你测出来，而它恰恰是
"性能优化"这四个字能不能立住的关键。测不出来就别写这一条。

---

## 9. 不该做的事（Write-up 红线）

- ❌ 不要写"基于 Hadoop 集群""分布式数仓" —— 单机 PySpark 就是单机，
  面试官问一句"几个节点、怎么分片"就露馅。
- ❌ 不要写"实时数仓" —— 这条链路是离线的。
- ❌ 不要写"日均 TB 级"（更别写 PB 级）—— 实测单日压缩后约 1.3 GB，如实说量级。
- ❌ 不要堆砌没测过的优化项。
- ✅ 正确姿势：**单机能处理到这个量级 + 说得出优化手段 + 有前后数字** ——
  可信度远高于虚的架构名词。
