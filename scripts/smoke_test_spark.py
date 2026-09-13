"""
99 · PySpark 环境冒烟测试

在写第 2 周的清洗脚本之前，先用它确认 Spark 能正常起来、能读能写。
必须用「文件方式」运行，不要用管道/heredoc 喂标准输入 ——
Windows 上 Spark 的 Python worker 会继承 stdin，从管道读脚本会导致 worker 崩溃。

用法：
    python scripts/smoke_test_spark.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from spark_env import get_spark
import config as C

spark = get_spark("smoke-test", shuffle_partitions=4)

print("=" * 56)
print("PySpark 冒烟测试")
print("=" * 56)
print(f"Spark 版本 : {spark.version}")
print(f"Java  版本 : {spark.sparkContext._jvm.System.getProperty('java.version')}")

# 1) 基本 DataFrame + shuffle（EOFException 通常在这一步暴露）
df = spark.createDataFrame([(i % 3, i) for i in range(1000)], ["k", "v"])
agg = df.groupBy("k").count().orderBy("k").collect()
print(f"shuffle 聚合 : {[(r['k'], r['count']) for r in agg]}")

# 2) 读取真实 ODS Parquet
hours = sorted(C.ODS_DIR.glob("dt=*/hour=*"))
if hours:
    p = spark.read.parquet(str(C.ODS_DIR / "dt=*" / "hour=*"))
    n = p.count()
    print(f"读取 ODS     : {n:,} 行，{len(p.columns)} 列")
    print(f"字段         : {p.columns}")

    # 3) 真实聚合，验证 payload 字符串能取出来
    from pyspark.sql import functions as F
    top = (p.groupBy("event_type").count()
             .orderBy(F.desc("count")).limit(5).collect())
    print("事件类型 Top5:")
    for r in top:
        print(f"   {r['event_type']:<32}{r['count']:>8,}")

    # 4) 写 Parquet（验证 winutils 生效）
    out = C.DATA_DIR / "_smoke_out"
    (p.select("event_id", "event_type").limit(1000)
      .write.mode("overwrite").parquet(str(out)))
    print(f"写 Parquet   : OK -> {out}")
    import shutil
    shutil.rmtree(out, ignore_errors=True)
else:
    print("说明：data/ods 为空，跳过 Parquet 读写测试（先跑 01/02 脚本）")

spark.stop()
print("=" * 56)
print("冒烟测试通过")
