"""
08 · Text-to-SQL Agent

把一个自然语言问题，变成能在 warehouse.duckdb 上跑的 SQL，并返回结果。

流程：
    问题 → 拼 schema + 少量示例 → LLM 生成 SQL → 安全校验 → 执行
         → 若报错，把错误信息回灌给 LLM 重试（最多 3 轮）
         → 仍失败则拒答

【为什么不用 LangChain】
    整条链路只是「一次 HTTP 调用 + 一次 SQL 执行 + 一个重试循环」，
    用标准库 + requests 就够了。引入 LangChain 只会让依赖变重、调试变难。
    这也是我在上一版 RAG 项目里踩过的坑：框架封装掉的恰恰是出问题时最需要看见的部分。

【降级设计】
    没有配置 API Key 时自动进入「模板模式」：用预置的关键词→SQL 映射回答常见问题。
    这样演示环境永远不会因为没配 Key 而开天窗。

配置（环境变量，均不写进代码）：
    TEXT2SQL_API_KEY    必填才启用 LLM 模式
    TEXT2SQL_BASE_URL   默认 https://api.deepseek.com/v1
    TEXT2SQL_MODEL      默认 deepseek-chat
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

MAX_RETRY = 3
MAX_ROWS = 200

# 只允许单条 SELECT / WITH 查询。Text-to-SQL 最大的风险就是模型生成写操作。
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|create|alter|truncate|attach|detach|copy|export|"
    r"install|load|pragma|call|set)\b", re.IGNORECASE)

# ─────────────────────────── 模板兜底 ───────────────────────────
# (触发关键词集合, 说明, SQL)。命中任一关键词即使用该模板。
TEMPLATES: list[tuple[set[str], str, str]] = [
    ({"机器人", "bot", "水军"},
     "机器人账号的活跃度占比",
     """SELECT
          CASE WHEN actor_login LIKE '%[bot]' OR actor_login LIKE '%-bot'
               THEN '机器人' ELSE '人类' END AS 类型,
          sum(event_cnt) AS 事件数,
          round(sum(event_cnt) * 100.0 / (SELECT sum(event_cnt) FROM ads.daily_actor_active), 2) AS 占比
        FROM ads.daily_actor_active GROUP BY 1 ORDER BY 2 DESC"""),

    ({"合并率", "pr 合并", "pr合并", "merge"},
     "每日 PR 合并率",
     """SELECT date,
          opened_cnt AS 新建, merged_cnt AS 已合并, closed_cnt AS 仅关闭,
          round(merged_cnt * 1.0 / nullif(merged_cnt + closed_cnt, 0), 4) AS 合并率
        FROM ads.daily_pr_funnel ORDER BY date"""),

    ({"仓库排行", "仓库榜", "top 仓库", "最活跃的仓库", "最忙的仓库", "仓库活跃"},
     "最活跃的仓库 Top10",
     """SELECT repo_name, sum(event_cnt) AS 事件数, sum(star_cnt) AS star, sum(pr_cnt) AS PR
        FROM ads.daily_repo_rank GROUP BY 1 ORDER BY 2 DESC LIMIT 10"""),

    ({"小时", "时段", "什么时候", "热力", "几点"},
     "每小时活跃分布（UTC）",
     """SELECT hour, sum(event_cnt) AS 事件数
        FROM ads.hourly_activity GROUP BY 1 ORDER BY 2 DESC"""),

    ({"组织", "公司", "机构"},
     "最活跃的组织 Top10",
     """SELECT org_login, sum(event_cnt) AS 事件数, sum(repo_cnt) AS 仓库数,
               sum(actor_cnt) AS 用户数
        FROM ads.daily_org_rank GROUP BY 1 ORDER BY 2 DESC LIMIT 10"""),

    ({"事件类型", "类型分布", "push", "pull request 多少"},
     "事件类型分布",
     """SELECT event_type, sum(event_cnt) AS 事件数
        FROM ads.daily_event_type GROUP BY 1 ORDER BY 2 DESC"""),

    ({"多少条", "总量", "总数", "数据量", "规模"},
     "数据规模总览",
     """SELECT
          (SELECT count(*) FROM dwd.gh_events_detail) AS 事件总行数,
          (SELECT count(DISTINCT repo_id) FROM dwd.gh_events_detail) AS 仓库数,
          (SELECT count(DISTINCT actor_id) FROM dwd.gh_events_detail) AS 用户数,
          (SELECT count(DISTINCT dt) FROM dwd.gh_events_detail) AS 覆盖天数"""),

    ({"用户", "贡献者", "谁最活跃", "开发者"},
     "最活跃的贡献者 Top10（已过滤机器人）",
     """SELECT actor_login, sum(event_cnt) AS 事件数, sum(repo_cnt) AS 触及仓库
        FROM ads.daily_actor_active
        WHERE actor_login NOT LIKE '%[bot]' AND actor_login NOT LIKE '%-bot'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10"""),
]

SCHEMA_HINT = """可用视图（DuckDB 语法）：

ads.daily_event_type(date, event_type, event_cnt, actor_cnt)
    日 × 事件类型的事件数与去重用户数

ads.daily_repo_rank(date, repo_id, repo_name, event_cnt, actor_cnt,
                    push_cnt, star_cnt, fork_cnt, pr_cnt, issue_cnt)
    日 × 仓库的各类事件计数，仓库活跃分析的**主表**

ads.daily_actor_active(date, actor_id, actor_login, event_cnt, repo_cnt,
                       push_cnt, pr_cnt, issue_cnt)
    日 × 用户的活跃度；actor_login 以 [bot] 或 -bot 结尾的是机器人账号

ads.daily_org_rank(date, org_login, event_cnt, repo_cnt, actor_cnt)
    日 × 组织

ads.hourly_activity(date, hour, event_cnt, actor_cnt, repo_cnt)
    日 × 小时（hour 为 0-23，UTC 时区）

ads.daily_pr_funnel(date, opened_cnt, merged_cnt, closed_cnt, reopened_cnt,
                    total_cnt, repo_cnt, actor_cnt, merge_rate)
    PR 漏斗。合并率 = merged_cnt / (merged_cnt + closed_cnt)

dwd.dim_repo(repo_id, repo_name, org_login, language, event_cnt,
             first_seen_dt, last_seen_dt)
dwd.dim_actor(actor_id, actor_login, event_cnt, first_seen_dt, last_seen_dt)
dwd.gh_events_detail(...)  事件事实表，1000 万行，**尽量别全表扫**

约定：
- date 是 DATE 类型，用 DATE '2026-09-10' 这种字面量
- 排行榜类查询务必加 ORDER BY + LIMIT
"""

SYSTEM_PROMPT = f"""你是一个 DuckDB SQL 专家。用户会用中文提问，你要生成一条可以**直接执行**的 SQL。

{SCHEMA_HINT}

硬性要求：
1. 只输出一条 SELECT（或 WITH）语句，不要任何解释文字、不要 markdown 代码块围栏。
2. 只使用上面列出的视图和字段，不要臆造表名或列名。
3. 除非用户明确要求，聚合查询都要带 ORDER BY 和 LIMIT。
4. 空值用 COALESCE / nullif 处理，避免除零。"""


@dataclass
class Result:
    question: str
    sql: str = ""
    rows: list = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    attempts: list[dict] = field(default_factory=list)
    mode: str = "llm"          # llm / template / refused
    error: str = ""
    elapsed: float = 0.0

    @property
    def ok(self) -> bool:
        return not self.error and bool(self.sql)


# ─────────────────────────── LLM 调用 ───────────────────────────
def llm_available() -> bool:
    return bool(os.getenv("TEXT2SQL_API_KEY"))


def _chat(messages: list[dict], temperature: float = 0.0) -> str:
    """调用 OpenAI 兼容的 /chat/completions。"""
    import requests

    base = os.getenv("TEXT2SQL_BASE_URL", "https://api.deepseek.com/v1").rstrip("/")
    model = os.getenv("TEXT2SQL_MODEL", "deepseek-chat")
    key = os.environ["TEXT2SQL_API_KEY"]

    resp = requests.post(
        f"{base}/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={"model": model, "messages": messages,
              "temperature": temperature, "max_tokens": 800},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _clean_sql(text: str) -> str:
    """把模型输出里可能夹带的 markdown 围栏和说明文字剥掉。"""
    text = text.strip()
    text = re.sub(r"^```(?:sql)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    m = re.search(r"\b(select|with)\b", text, re.IGNORECASE)
    if m:
        text = text[m.start():]
    return text.strip().rstrip(";").strip()


def is_safe(sql: str) -> tuple[bool, str]:
    if not sql:
        return False, "SQL 为空"
    if _FORBIDDEN.search(sql):
        return False, f"包含被禁止的关键字（仅允许只读查询）"
    if not re.match(r"^\s*(select|with)\b", sql, re.IGNORECASE):
        return False, "不是 SELECT/WITH 查询"
    return True, ""


# ─────────────────────────── 执行 ───────────────────────────
def _execute(con, sql: str) -> tuple[list, list[str]]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchmany(MAX_ROWS)
    return rows, cols


def _match_template(question: str) -> tuple[str, str] | None:
    """按「命中关键词总长度」打分选模板，而不是按列表顺序取第一个。

    否则泛词会抢走具体词：比如「每小时什么时段最活跃」里同时含「活跃」和「时段」，
    按顺序匹配会错误命中仓库榜。用长度加权后，「时段+小时」明显比「活跃」更具体。
    """
    q = question.lower()
    best, best_score = None, 0
    for keywords, desc, sql in TEMPLATES:
        hits = [k for k in keywords if k.lower() in q]
        if not hits:
            continue
        score = sum(len(k) for k in hits) + 2 * len(hits)
        if score > best_score:
            best_score, best = score, (desc, sql)
    return best


def ask(question: str, con=None, history: list[dict] | None = None) -> Result:
    """主入口：自然语言问题 → SQL → 结果。"""
    t0 = time.time()
    own_con = con is None
    if own_con:
        if not C.WAREHOUSE_DB.exists():
            return Result(question, mode="refused", error="warehouse.duckdb 不存在")
        con = duckdb.connect(str(C.WAREHOUSE_DB), read_only=True)

    res = Result(question=question)

    # ── 没有 API Key：走模板模式，保证演示不空转 ──
    if not llm_available():
        res.mode = "template"
        hit = _match_template(question)
        if not hit:
            res.error = ("未配置 TEXT2SQL_API_KEY，当前为模板模式。"
                         "问题没匹配到任何模板，请换个问法或配置 API Key。")
            res.elapsed = time.time() - t0
            return res
        desc, sql = hit
        res.sql = sql
        try:
            res.rows, res.columns = _execute(con, sql)
            res.attempts.append({"round": 1, "sql": sql, "ok": True, "note": f"模板：{desc}"})
        except Exception as exc:
            res.error = f"{type(exc).__name__}: {exc}"
        res.elapsed = time.time() - t0
        return res

    # ── LLM 模式：生成 → 执行 → 失败带错误重试 ──
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    if history:
        messages.extend(history[-2:])
    messages.append({"role": "user", "content": question})

    sql = ""
    for attempt in range(1, MAX_RETRY + 1):
        try:
            raw = _chat(messages)
        except Exception as exc:
            res.error = f"调用模型失败：{type(exc).__name__}: {exc}"
            res.attempts.append({"round": attempt, "sql": "", "ok": False,
                                 "note": res.error})
            break

        sql = _clean_sql(raw)
        safe, why = is_safe(sql)
        if not safe:
            res.attempts.append({"round": attempt, "sql": sql, "ok": False, "note": why})
            messages.append({"role": "assistant", "content": raw})
            messages.append({"role": "user", "content":
                             f"这条 SQL 被拒绝了：{why}。请只生成一条 SELECT 语句。"})
            continue

        try:
            rows, cols = _execute(con, sql)
            res.sql, res.rows, res.columns = sql, rows, cols
            res.attempts.append({"round": attempt, "sql": sql, "ok": True, "note": "执行成功"})
            res.error = ""
            break
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            res.attempts.append({"round": attempt, "sql": sql, "ok": False, "note": err})
            res.sql = sql
            res.error = err
            # 关键：把数据库的原始报错回灌给模型，它才知道哪个列名写错了
            messages.append({"role": "assistant", "content": raw})
            messages.append({"role": "user", "content":
                             f"执行报错：{err}\n请根据上面的 schema 修正后重新生成一条 SQL。"})

    if res.error and not res.rows:
        res.mode = "refused"
    res.elapsed = time.time() - t0
    if own_con:
        con.close()
    return res


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "最近一周最活跃的仓库有哪些"
    r = ask(q)
    print(f"模式：{r.mode}   耗时：{r.elapsed:.2f}s")
    print(f"问题：{r.question}")
    print(f"SQL：\n{r.sql}\n")
    if r.error:
        print("错误：", r.error)
    if r.rows:
        print(" | ".join(r.columns))
        for row in r.rows[:10]:
            print(" | ".join(str(v) for v in row))
        print(f"... 共 {len(r.rows)} 行（上限 {MAX_ROWS}）")
