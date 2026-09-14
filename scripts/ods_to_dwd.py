"""
03 · ODS → DWD（PySpark）

输入：data/ods/dt=*/hour=*/*.parquet      （顶层字段 + payload 原始 JSON）
输出：
    data/dwd/gh_events_detail/            事件事实表（payload 已解析）
    data/dwd/dim_repo/                    仓库维度
    data/dwd/dim_actor/                   用户维度
    data/dwd/dim_date/                    日期维度

设计要点：
  1. payload 用 get_json_object 按路径取值，而不是 from_json 定 schema。
     原因：GitHub Archive 有 15+ 种事件类型，payload 结构各不相同，定 schema
     必然出现结构不匹配；按路径取缺失字段只会返回 NULL，不会失败。
  2. 事实表按 dt 分区写出，供下游按天裁剪。
  3. 维度表是「全量快照」而非拉链表 —— 4 周版不做 SCD。

⚠️ 必须用 Python 3.11 环境运行（见 spark_env.py 的说明）：
    D:\\1\\anaconda3\\envs\\spark\\python.exe scripts/ods_to_dwd.py
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C
from spark_env import get_spark

from pyspark.sql import DataFrame, functions as F

# 只保留有分析价值的事件类型（其余类型 payload 字段太少，进事实表是噪音）
EVENT_TYPES = [
    "PushEvent", "PullRequestEvent", "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent", "IssuesEvent", "IssueCommentEvent",
    "WatchEvent", "ForkEvent", "CreateEvent", "DeleteEvent",
    "ReleaseEvent", "CommitCommentEvent", "PublicEvent",
    "MemberEvent", "GollumEvent",
]


# 所有字段一律可空 —— 不同事件类型的 payload 结构不一样，缺字段自然变 NULL，
# 不会因为结构不匹配而失败。这就是不用 get_json_object 逐字段取的原因。
PAYLOAD_SCHEMA = (
    "STRUCT<"
    "  size:INT,"
    "  distinct_size:INT,"
    "  ref:STRING,"
    "  ref_type:STRING,"
    "  action:STRING,"
    "  number:INT,"
    "  pull_request:STRUCT<base:STRUCT<repo:STRUCT<language:STRING>>>,"
    "  issue:STRUCT<number:INT>,"
    "  forkee:STRUCT<language:STRING>,"
    "  repository:STRUCT<language:STRING>"
    ">"
)


def parse_payload(ods: DataFrame) -> DataFrame:
    """把 payload JSON 里分散在各事件类型中的字段统一抽成列。

    【性能教训】一开始用 16 次 `get_json_object(payload, '$.xxx')` 取字段，
    结果必然 OOM（GC overhead limit exceeded / Java heap space）。
    原因：get_json_object 每调用一次就要把整段 JSON 重新解析一遍，而部分
    PushEvent 的 payload 有几 MB（几十个 commit），16 次重解析直接把内存打爆。
    改成 `from_json` **一次解析整行**、再取 struct 字段，内存和 CPU 都降一个量级。
    """
    p = F.from_json("payload", PAYLOAD_SCHEMA)

    return ods.select(
        "event_id",
        "event_type",
        "actor_id",
        "actor_login",
        "repo_id",
        "repo_name",
        F.coalesce("org_login", F.lit("")).alias("org_login"),
        F.to_timestamp("created_at").alias("event_ts"),
        # dt 取自 ODS 的 Hive 分区列（写入时就是按这个分区的），而不是从
        # created_at 反推 —— 保证事实表的分区和上游严格对齐，也便于按天增量处理。
        F.to_date("dt").alias("dt"),

        # PushEvent
        p["size"].alias("push_size"),
        p["distinct_size"].alias("push_distinct_size"),
        p["ref"].alias("push_ref"),

        # PullRequestEvent / IssuesEvent / ReleaseEvent 共用的 action
        p["action"].alias("action"),

        # PullRequestEvent
        p["number"].alias("pr_number"),

        # 语言只在这几类事件里才有（真实数据局限，见 docs/DESIGN.md §3.2）
        F.coalesce(
            p["pull_request"]["base"]["repo"]["language"],
            p["forkee"]["language"],
            p["repository"]["language"],
        ).alias("language"),

        # IssuesEvent / IssueCommentEvent
        p["issue"]["number"].alias("issue_number"),

        # CreateEvent / DeleteEvent
        p["ref_type"].alias("create_ref_type"),
    )


def build_fact(df: DataFrame) -> DataFrame:
    """按事件类型把通用的 action 列拆成语义明确的列，并过滤到白名单类型。"""
    return (df
            .filter(F.col("event_type").isin(EVENT_TYPES))
            .filter(F.col("event_id").isNotNull() & (F.col("event_id") != ""))
            .withColumn("pr_action",
                        F.when(F.col("event_type") == "PullRequestEvent", F.col("action")))
            .withColumn("issue_action",
                        F.when(F.col("event_type").isin("IssuesEvent", "IssueCommentEvent"),
                               F.col("action")))
            .withColumn("release_action",
                        F.when(F.col("event_type") == "ReleaseEvent", F.col("action")))
            .drop("action"))


def build_dim_repo(fact: DataFrame) -> DataFrame:
    """仓库维度：最近一次出现的名称 / 组织 / 语言 + 首末出现日期。"""
    base = fact.groupBy("repo_id").agg(
        F.count("*").alias("event_cnt"),
        F.min("dt").alias("first_seen_dt"),
        F.max("dt").alias("last_seen_dt"),
    )
    name = (fact.filter(F.col("repo_name") != "")
                .groupBy("repo_id")
                .agg(F.max_by("repo_name", "event_ts").alias("repo_name")))
    org = (fact.filter(F.col("org_login") != "")
               .groupBy("repo_id")
               .agg(F.max_by("org_login", "event_ts").alias("org_login")))
    lang = (fact.filter(F.col("language").isNotNull())
                .groupBy("repo_id")
                .agg(F.max_by("language", "event_ts").alias("language")))

    return (base.join(name, "repo_id", "left")
                .join(org, "repo_id", "left")
                .join(lang, "repo_id", "left")
                .select("repo_id", "repo_name", "org_login", "language",
                        "event_cnt", "first_seen_dt", "last_seen_dt"))


def build_dim_actor(fact: DataFrame) -> DataFrame:
    base = fact.groupBy("actor_id").agg(
        F.count("*").alias("event_cnt"),
        F.min("dt").alias("first_seen_dt"),
        F.max("dt").alias("last_seen_dt"),
    )
    name = (fact.filter(F.col("actor_login") != "")
                .groupBy("actor_id")
                .agg(F.max_by("actor_login", "event_ts").alias("actor_login")))
    return (base.join(name, "actor_id", "left")
                .select("actor_id", "actor_login", "event_cnt",
                        "first_seen_dt", "last_seen_dt"))


def build_dim_date(fact: DataFrame):
    bounds = fact.agg(F.min("dt").alias("lo"), F.max("dt").alias("hi")).collect()[0]
    if bounds["lo"] is None:
        return None
    days = F.sequence(F.lit(bounds["lo"]), F.lit(bounds["hi"]), F.expr("interval 1 day"))
    # explode() 是生成器，不能再套 cast() —— 会报 UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS。
    # array<date> 展开后本来就是 date，无需转换。
    df = fact.select(F.explode(days).alias("date")).distinct()
    return (df
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
            .withColumn("week_of_year", F.weekofyear("date"))
            .withColumn("weekday", F.dayofweek("date"))          # 1=周日
            .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
            .select("date", "year", "month", "day", "week_of_year",
                    "weekday", "is_weekend"))


def write_fact_by_day(spark, ods: DataFrame, days: list, overwrite: bool) -> int:
    """按天写事实表：每天一个 Spark 作业，写进同一个 dt= 分区目录。

    【为什么按天】一次性把 7 天 1,013 万行全部 parse + partitionBy 写出，
    实测稳定复现 OOM（java.lang.OutOfMemoryError: GC overhead limit exceeded）。
    按天切分后每批只有 ~150 万行，内存压力降一个数量级，而且这正是真实
    数仓里「日批增量」的标准形态 —— 每天跑一次，追加一个 dt 分区。
    """
    out = C.DWD_DIR / "gh_events_detail"
    if out.exists() and not overwrite:
        n = spark.read.parquet(str(out)).count()
        print(f"  {'gh_events_detail':22s} {n:>10,} 行   （已存在，跳过）")
        return n
    if out.exists():
        shutil.rmtree(out, ignore_errors=True)

    t0 = time.time()
    total = 0
    for i, day in enumerate(days, 1):
        one = build_fact(parse_payload(ods.filter(F.col("dt") == day)))
        one.write.mode("append").partitionBy("dt").parquet(str(out))
        c = one.count()
        total += c
        print(f"    [{i}/{len(days)}] {day}  {c:>10,} 行")

    size = sum(f.stat().st_size for f in out.rglob("*.parquet")) / 1024 / 1024
    print(f"  {'gh_events_detail':22s} {total:>10,} 行   {size:8.1f} MB   "
          f"{time.time() - t0:5.1f}s")
    return total


def write(spark, df: DataFrame, name: str, partition_by=None, overwrite: bool = True) -> int:
    """写出一张表，并统计行数/体积。

    注意：行数是从**写好的 Parquet** 读回来数的，不是 df.count()。
    后者会让整条上游血缘（含 payload 解析）重算一遍，对千万行是灾难。
    """
    out = C.DWD_DIR / name
    if out.exists() and not overwrite:
        n = spark.read.parquet(str(out)).count()
        size = sum(f.stat().st_size for f in out.rglob("*.parquet")) / 1024 / 1024
        print(f"  {name:22s} {n:>10,} 行   {size:8.1f} MB   （已存在，跳过）")
        return n
    if out.exists():
        shutil.rmtree(out, ignore_errors=True)
    t0 = time.time()
    w = df.write.mode("overwrite")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.parquet(str(out))
    n = spark.read.parquet(str(out)).count()
    size = sum(f.stat().st_size for f in out.rglob("*.parquet")) / 1024 / 1024
    print(f"  {name:22s} {n:>10,} 行   {size:8.1f} MB   {time.time() - t0:5.1f}s")
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description="ODS → DWD")
    ap.add_argument("--shuffle-partitions", type=int, default=32,
                    help="Spark shuffle 分区数。Spark 默认 200 对这种单机数据量过度切分，"
                         "但也不能太小（实测 8 时单个 task 要扛百万行，容易 OOM）")
    ap.add_argument("--overwrite", action="store_true",
                    help="强制重算已存在的表。默认跳过已存在的表，便于快速迭代")
    args = ap.parse_args()

    if not list(C.ODS_DIR.glob("dt=*")):
        print(f"{C.ODS_DIR} 下没有数据，请先运行 scripts/raw_to_ods.py")
        return 1

    spark = get_spark("ods_to_dwd", shuffle_partitions=args.shuffle_partitions)
    t_all = time.time()

    print("=" * 62)
    print("ODS → DWD")
    print("=" * 62)

    t0 = time.time()
    # 【注意】必须读 ODS 根目录。写成 `dt=*/hour=*` 通配符时 Spark 不会推断
    # Hive 分区列，结果拿不到 dt/hour —— 会报 UNRESOLVED_COLUMN。
    ods = spark.read.parquet(str(C.ODS_DIR))
    n_ods = ods.count()
    print(f"读取 ODS          {n_ods:,} 行   ({time.time() - t0:.1f}s)")
    print(f"shuffle 分区数    {spark.conf.get('spark.sql.shuffle.partitions')}")

    # ── 事实表：构建 → 落盘。全程不 cache ──────────────────────────────
    # 曾经写成 `build_fact(...).cache()` 再 count()，结果 GC overhead limit exceeded
    # + java.lang.OutOfMemoryError —— 千万行的解析结果放不进内存，也不该往内存放。
    print("-" * 62)
    print("写出 DWD 事实表（按天增量）：")
    days = [r[0] for r in ods.select("dt").distinct().orderBy("dt").collect()]
    t0 = time.time()
    n_fact = write_fact_by_day(spark, ods, days, args.overwrite)
    n_drop = n_ods - n_fact
    print(f"  过滤掉 {n_drop:,} 行（{n_drop / max(n_ods, 1) * 100:.1f}%），"
          f"耗时 {time.time() - t0:.1f}s")

    # ── 维度表：从写好的事实表读回再聚合 ────────────────────────────
    # 「先物化、再派生」是数仓的常规做法：上游只算一次，下游多次消费。
    fact_dwd = spark.read.parquet(str(C.DWD_DIR / "gh_events_detail"))

    print("写出 DWD 维度表：")
    write(spark, build_dim_repo(fact_dwd), "dim_repo", overwrite=args.overwrite)
    write(spark, build_dim_actor(fact_dwd), "dim_actor", overwrite=args.overwrite)
    d = build_dim_date(fact_dwd)
    if d is not None:
        write(spark, d, "dim_date", overwrite=args.overwrite)

    # 语言覆盖率是真实数据局限，打印出来，写文档时引用
    n_lang = fact_dwd.filter(F.col("language").isNotNull()).count()
    print("-" * 62)
    print(f"带语言信息的事件  {n_lang:,} / {n_fact:,}  ({n_lang / max(n_fact, 1) * 100:.1f}%)")
    print(f"事实表分区数      {fact_dwd.select('dt').distinct().count()}")
    print(f"总耗时            {time.time() - t_all:.1f}s")
    print("=" * 62)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
