"""
PySpark 会话工厂 —— 统一处理 Windows 下的环境依赖。

背景：Windows 上直接 `SparkSession.builder.getOrCreate()`，第一次 shuffle 时
会报 `java.io.EOFException`（Python worker 启动即崩溃）。根因有三个，缺一不可：

  1. 缺 winutils.exe → 必须设 HADOOP_HOME 和 hadoop.home.dir
  2. Python worker 找不到解释器 → 必须设 PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON
  3. worker 回连 driver 时走错网卡 → 设 SPARK_LOCAL_IP=127.0.0.1

本模块把这三件事固定下来，其它脚本一律 `from spark_env import get_spark`。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# 优先用完整 Hadoop 安装；退回 winutils 单独包
HADOOP_CANDIDATES = [
    Path(r"D:\1\hadoop-3.1.3"),
    Path(r"D:\1\apache-hadoop-3.1.3-winutils-m\apache-hadoop-3.1.3-winutils-master"),
]
PYTHON = Path(sys.executable)


def _find_hadoop() -> Path | None:
    for p in HADOOP_CANDIDATES:
        if (p / "bin" / "winutils.exe").exists():
            return p
    return None


def setup_env() -> None:
    """把 Windows 上 PySpark 需要的环境变量补齐。"""
    # 系统里已配好 HADOOP_HOME 且有效时就用它，不要覆盖（少一层出错的余地）
    existing = os.environ.get("HADOOP_HOME")
    if existing and (Path(existing) / "bin" / "winutils.exe").exists():
        hadoop = Path(existing)
    else:
        hadoop = _find_hadoop()

    if hadoop:
        os.environ["HADOOP_HOME"] = str(hadoop)
        os.environ["hadoop.home.dir"] = str(hadoop)
        bin_dir = str(hadoop / "bin")
        if bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
    else:
        print(f"[warn] 未找到 winutils，候选路径：{[str(p) for p in HADOOP_CANDIDATES]}",
              file=sys.stderr)

    # worker 必须用同一个解释器，否则 ABI 不一致会直接崩
    os.environ["PYSPARK_PYTHON"] = str(PYTHON)
    os.environ["PYSPARK_DRIVER_PYTHON"] = str(PYTHON)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    # 【关键】清掉继承来的 PYTHONPATH。
    # 若宿主环境（IDE / 沙箱 / 某些安全工具）把 PYTHONPATH 指向了带 sitecustomize.py
    # 的目录，Python 每次启动都会执行那个脚本，Spark 的 Python worker 会被它拖崩，
    # 现象就是 "Python worker exited unexpectedly (crashed)" + java.io.EOFException。
    # PySpark 自己会给 worker 注入正确的 PYTHONPATH，不需要外部提供。
    os.environ.pop("PYTHONPATH", None)


def get_spark(app_name: str = "gh-eco-platform", shuffle_partitions: int = 8,
              master: str = "local[*]"):
    """创建/复用 SparkSession。

    shuffle_partitions 默认给 8 而不是 Spark 默认的 200 —— 我们的数据量在单机上
    跑，200 个分区会产生大量空任务，这是 §6 里记录在案的优化点之一。
    """
    setup_env()
    from pyspark.sql import SparkSession

    # 【注意】必须用 POSIX 风格的正斜杠。把 Windows 反斜杠路径塞进 -D 参数时，
    # 反斜杠会被逐层转义吞掉，最终变成 "D:1hadoop-3.1.3" 这种非法路径，
    # Spark 会报 "Hadoop home directory ... is not an absolute path"。
    hadoop = os.environ.get("HADOOP_HOME")
    if hadoop:
        hp = Path(hadoop).as_posix()
        java_opts = f"-Dhadoop.home.dir={hp} -Djava.library.path={hp}/bin -Dfile.encoding=UTF-8"
    else:
        java_opts = "-Dfile.encoding=UTF-8"

    spark = (SparkSession.builder
             .master(master)
             .appName(app_name)
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
             .config("spark.driver.extraJavaOptions", java_opts)
             .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
             .config("spark.sql.warehouse.dir", str(ROOT / "spark-warehouse"))
             # 双保险：从 Spark 配置层再清一次 worker 的 PYTHONPATH
             .config("spark.executorEnv.PYTHONPATH", "")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
