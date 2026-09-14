"""
04 · DWD → ADS（PySpark）

输入：data/dwd/gh_events_detail   （1,013 万行事件事实表）
输出：data/ads/                   6 张面向分析的应用宽表

    daily_event_type     日 × 事件类型      事件分布
    daily_repo_rank      日 × 仓库          仓库活跃榜
    daily_actor_active   日 × 用户          贡献者活跃度
    daily_org_rank       日 × 组织          组织榜
    hourly_activity      日 × 小时          活跃热力
    daily_pr_funnel      日 × PR 动作       PR 漏斗

⚠️ 语言趋势表已被移除：实测 GitHub Archive 的 pull_request 字段被裁剪，
   不含 language，全量只有 0.04% 的事件带语言。详见 docs/DESIGN.md §3.2。

⚠️ 必须用 Python 3.11 环境运行：
    D:\\1\\anaconda3\\envs\\spark\\python.exe scripts/dwd_to_ads.py
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

# 每张表的「事件计数」列：事件类型 -> 目标列名
CNT_MAP = {
    "PushEvent": "push_cnt",
    "WatchEvent": "star_cnt",
    "ForkEvent": "fork_cnt",
    "PullRequestEvent": "pr_cnt",
    "IssuesEvent": "issue_cnt",
}


def _pivot_counts(df: DataFrame, group_cols: list[str], prefix: str) -> DataFrame:
    """把指定事件类型按天/维度转成并列的计数列。"""
    exprs = [
        F.sum(F.when(F.col("event_type") == et, 1).otherwise(0)).alias(f"{prefix}{col}")
        for et, col in CNT_MAP.items()
    ]
    return df.groupBy(*group_cols).agg(*exprs)


def build_daily_event_type(fact: DataFrame) -> DataFrame:
    return (fact.groupBy("dt", "event_type")
                .agg(F.count("*").alias("event_cnt"),
                     F.countDistinct("actor_id").alias("actor_cnt"))
                .withColumnRenamed("dt", "date"))


def build_daily_repo_rank(fact: DataFrame) -> DataFrame:
    base = fact.groupBy("dt", "repo_id", "repo_name").agg(
        F.count("*").alias("event_cnt"),
        F.countDistinct("actor_id").alias("actor_cnt"),
    )
    pivot = _pivot_counts(fact, ["dt", "repo_id"], "")
    return (base.join(pivot, ["dt", "repo_id"], "left")
                .select("dt", "repo_id", "repo_name", "event_cnt", "actor_cnt",
                        "push_cnt", "star_cnt", "fork_cnt", "pr_cnt", "issue_cnt")
                .withColumnRenamed("dt", "date"))


def build_daily_actor_active(fact: DataFrame) -> DataFrame:
    base = fact.groupBy("dt", "actor_id", "actor_login").agg(
        F.count("*").alias("event_cnt"),
        F.countDistinct("repo_id").alias("repo_cnt"),
    )
    pivot = _pivot_counts(fact, ["dt", "actor_id"], "")
    return (base.join(pivot, ["dt", "actor_id"], "left")
                .select("dt", "actor_id", "actor_login", "event_cnt", "repo_cnt",
                        "push_cnt", "pr_cnt", "issue_cnt")
                .withColumnRenamed("dt", "date"))


def build_daily_org_rank(fact: DataFrame) -> DataFrame:
    return (fact.filter(F.col("org_login") != "")
                .groupBy("dt", "org_login")
                .agg(F.count("*").alias("event_cnt"),
                     F.countDistinct("repo_id").alias("repo_cnt"),
                     F.countDistinct("actor_id").alias("actor_cnt"))
                .withColumnRenamed("dt", "date"))


def build_hourly_activity(fact: DataFrame) -> DataFrame:
    return (fact.withColumn("hour", F.hour("event_ts"))
                .groupBy("dt", "hour")
                .agg(F.count("*").alias("event_cnt"),
                     F.countDistinct("actor_id").alias("actor_cnt"),
                     F.countDistinct("repo_id").alias("repo_cnt"))
                .withColumnRenamed("dt", "date"))


def build_daily_pr_funnel(fact: DataFrame) -> DataFrame:
    """PR 漏斗：日的 opened / merged / closed 计数与合并率。

    【数据源修正】原设计想用 `payload.pull_request.merged` 判断是否合并，
    实测该字段在 GitHub Archive 里被裁剪掉了（`pull_request` 只剩
    base/head/id/number/url 四个子字段，merged 非空率 0%）。
    真正的合并信号是 **`payload.action == "merged"`** —— 实测 7 天里
    有 471,838 条，是完整可用的。

    合并率 = merged / (merged + closed)，即「关闭的 PR 里有多少是被合并的」。
    """
    pr = fact.filter(F.col("event_type") == "PullRequestEvent")
    agg = pr.groupBy("dt").agg(
        F.sum(F.when(F.col("pr_action") == "opened", 1).otherwise(0)).alias("opened_cnt"),
        F.sum(F.when(F.col("pr_action") == "merged", 1).otherwise(0)).alias("merged_cnt"),
        F.sum(F.when(F.col("pr_action") == "closed", 1).otherwise(0)).alias("closed_cnt"),
        F.sum(F.when(F.col("pr_action") == "reopened", 1).otherwise(0)).alias("reopened_cnt"),
        F.count("*").alias("total_cnt"),
        F.countDistinct("repo_id").alias("repo_cnt"),
        F.countDistinct("actor_id").alias("actor_cnt"),
    )
    done = F.col("merged_cnt") + F.col("closed_cnt")
    return (agg
            .withColumn("merge_rate",
                        F.when(done > 0, F.round(F.col("merged_cnt") / done, 4)))
            .withColumnRenamed("dt", "date"))


TABLES = {
    "daily_event_type": build_daily_event_type,
    "daily_repo_rank": build_daily_repo_rank,
    "daily_actor_active": build_daily_actor_active,
    "daily_org_rank": build_daily_org_rank,
    "hourly_activity": build_hourly_activity,
    "daily_pr_funnel": build_daily_pr_funnel,
}


def write(spark, df: DataFrame, name: str, overwrite: bool) -> int:
    out = C.ADS_DIR / name
    if out.exists() and not overwrite:
        n = spark.read.parquet(str(out)).count()
        print(f"  {name:22s} {n:>9,} 行   （已存在，跳过）")
        return n
    if out.exists():
        shutil.rmtree(out, ignore_errors=True)
    t0 = time.time()
    # ADS 表都很小（最多十万行级），合成单文件，下游读取和分发都省事
    df.coalesce(1).write.mode("overwrite").parquet(str(out))
    n = spark.read.parquet(str(out)).count()
    size = sum(f.stat().st_size for f in out.rglob("*.parquet")) / 1024 / 1024
    print(f"  {name:22s} {n:>9,} 行   {size:7.2f} MB   {time.time() - t0:5.1f}s")
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description="DWD → ADS")
    ap.add_argument("--shuffle-partitions", type=int, default=32)
    ap.add_argument("--overwrite", action="store_true", help="强制重算（默认跳过已存在的表）")
    args = ap.parse_args()

    dwd = C.DWD_DIR / "gh_events_detail"
    if not dwd.exists():
        print(f"{dwd} 不存在，请先运行 scripts/ods_to_dwd.py")
        return 1

    spark = get_spark("dwd_to_ads", shuffle_partitions=args.shuffle_partitions)
    t_all = time.time()

    print("=" * 62)
    print("DWD → ADS")
    print("=" * 62)

    t0 = time.time()
    fact = spark.read.parquet(str(dwd))
    n = fact.count()
    print(f"读取事实表      {n:,} 行   ({time.time() - t0:.1f}s)")

    # 刻意不 cache：事实表 1,013 万行，cache 进堆容易 OOM（前一版就踩过）。
    # 6 张表各自从 Parquet 重读一遍（本地盘 361 MB，很快），换内存安全。
    # 想优化的话，这里正是 §6「性能优化」的试验田 —— 缓存策略 / 重分区 / AQE。

    print("-" * 62)
    print("写出 ADS：")
    total = 0
    for name, fn in TABLES.items():
        total += write(spark, fn(fact), name, args.overwrite)

    print("-" * 62)
    print(f"6 张表合计      {total:,} 行")
    print(f"总耗时          {time.time() - t_all:.1f}s")
    print("=" * 62)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
