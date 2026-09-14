"""
07 · Streamlit 可视化看板

数据源：warehouse.duckdb（由 scripts/build_duckdb.py 生成）
启动：
    D:\\1\\anaconda3\\python.exe -m streamlit run app/dashboard.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

st.set_page_config(page_title="GitHub 开源生态数据分析", page_icon="📊", layout="wide")

FONT = dict(family="Microsoft YaHei, SimHei, sans-serif", size=13)
BLUE = "#3B6FD4"
ACCENT = "#D85A30"


@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    if not C.WAREHOUSE_DB.exists():
        st.error(f"找不到 {C.WAREHOUSE_DB.name}，请先运行 `python scripts/build_duckdb.py`")
        st.stop()
    return duckdb.connect(str(C.WAREHOUSE_DB), read_only=True)


@st.cache_data(ttl=600)
def q(sql: str) -> pd.DataFrame:
    return get_con().execute(sql).df()


def style(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(font=FONT, height=height, margin=dict(l=10, r=10, t=40, b=10),
                      plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                      legend=dict(orientation="h", y=-0.2))
    return fig


# ──────────────────────────── 侧边栏 ────────────────────────────
st.sidebar.title("筛选")

dates = q("SELECT DISTINCT date FROM ads.daily_event_type ORDER BY date")["date"].tolist()
if not dates:
    st.error("ADS 层没有数据，请先跑完 01→04 脚本")
    st.stop()

lo, hi = st.sidebar.select_slider(
    "日期范围", options=dates, value=(dates[0], dates[-1]))
hide_bots = st.sidebar.checkbox("过滤机器人账号", value=True,
                                help="过滤 login 以 [bot] 结尾或以 -bot 结尾的账号")
top_n = st.sidebar.slider("排行榜条数", 5, 30, 15)

d0, d1 = str(lo), str(hi)

# ──────────────────────────── 顶部指标 ────────────────────────────
st.title("GitHub 开源生态数据分析")
st.caption(f"数据源 GitHub Archive ｜ 当前范围 {d0} ~ {d1}")

m = q(f"""
    SELECT count(*) AS events,
           count(DISTINCT repo_id) AS repos,
           count(DISTINCT actor_id) AS actors,
           count(DISTINCT dt) AS days
    FROM dwd.gh_events_detail WHERE dt BETWEEN DATE '{d0}' AND DATE '{d1}'
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("事件总数", f"{m.events:,.0f}")
c2.metric("覆盖天数", f"{m.days:.0f}")
c3.metric("活跃仓库", f"{m.repos:,.0f}")
c4.metric("活跃用户", f"{m.actors:,.0f}")

# ──────────────────────────── 1. 事件类型分布 ────────────────────────────
st.subheader("事件类型分布")
ev = q(f"""
    SELECT date, event_type, sum(event_cnt) AS cnt
    FROM ads.daily_event_type WHERE date BETWEEN DATE '{d0}' AND DATE '{d1}'
    GROUP BY 1, 2 ORDER BY 1
""")
top_types = (ev.groupby("event_type")["cnt"].sum()
               .nlargest(7).index.tolist())
ev["类型"] = ev["event_type"].where(ev["event_type"].isin(top_types), "其他")
area = ev.groupby(["date", "类型"], as_index=False)["cnt"].sum()
fig = px.area(area, x="date", y="cnt", color="类型",
              labels={"date": "日期", "cnt": "事件数"})
st.plotly_chart(style(fig), use_container_width=True)

# ──────────────────────────── 2 & 3. 仓库榜 / 用户榜 ────────────────────
left, right = st.columns(2)

with left:
    st.subheader(f"仓库活跃榜 Top{top_n}")
    repo = q(f"""
        SELECT repo_name, sum(event_cnt) AS 事件数,
               sum(star_cnt) AS star, sum(pr_cnt) AS PR
        FROM ads.daily_repo_rank WHERE date BETWEEN DATE '{d0}' AND DATE '{d1}'
        GROUP BY 1 ORDER BY 2 DESC LIMIT {top_n}
    """)
    fig = px.bar(repo.sort_values("事件数"), x="事件数", y="repo_name",
                 orientation="h", labels={"repo_name": ""},
                 color_discrete_sequence=[BLUE])
    st.plotly_chart(style(fig, 480), use_container_width=True)

with right:
    st.subheader(f"贡献者活跃榜 Top{top_n}")
    bot_clause = ("AND actor_login NOT LIKE '%[bot]' AND actor_login NOT LIKE '%-bot'"
                  if hide_bots else "")
    act = q(f"""
        SELECT actor_login, sum(event_cnt) AS 事件数, sum(repo_cnt) AS 触及仓库
        FROM ads.daily_actor_active
        WHERE date BETWEEN DATE '{d0}' AND DATE '{d1}' {bot_clause}
        GROUP BY 1 ORDER BY 2 DESC LIMIT {top_n}
    """)
    fig = px.bar(act.sort_values("事件数"), x="事件数", y="actor_login",
                 orientation="h", labels={"actor_login": ""},
                 color_discrete_sequence=[ACCENT])
    st.plotly_chart(style(fig, 480), use_container_width=True)
    if hide_bots:
        st.caption("已过滤 `*[bot]` 账号。关掉侧边栏开关可以看到机器人有多能刷。")

# ──────────────────────────── 4. 组织榜 ────────────────────────────
st.subheader(f"组织活跃榜 Top{top_n}")
org = q(f"""
    SELECT org_login, sum(event_cnt) AS 事件数,
           sum(repo_cnt) AS 仓库数, sum(actor_cnt) AS 用户数
    FROM ads.daily_org_rank WHERE date BETWEEN DATE '{d0}' AND DATE '{d1}'
    GROUP BY 1 ORDER BY 2 DESC LIMIT {top_n}
""")
fig = px.bar(org.sort_values("事件数"), x="事件数", y="org_login", orientation="h",
             labels={"org_login": ""}, color_discrete_sequence=[BLUE],
             hover_data=["仓库数", "用户数"])
st.plotly_chart(style(fig, 460), use_container_width=True)

# ──────────────────────────── 5. 小时活跃热力 ────────────────────────────
st.subheader("每小时活跃热力（UTC）")
hr = q(f"""
    SELECT date, hour, sum(event_cnt) AS cnt
    FROM ads.hourly_activity WHERE date BETWEEN DATE '{d0}' AND DATE '{d1}'
    GROUP BY 1, 2
""")
if not hr.empty:
    piv = hr.pivot(index="date", columns="hour", values="cnt").fillna(0)
    fig = px.imshow(piv, aspect="auto", color_continuous_scale="Blues",
                    labels=dict(x="小时 (UTC)", y="日期", color="事件数"))
    fig.update_xaxes(dtick=1)
    st.plotly_chart(style(fig, max(200, 60 * len(piv) + 120)), use_container_width=True)
    peak = hr.loc[hr["cnt"].idxmax()]
    low = hr.loc[hr["cnt"].idxmin()]
    st.caption(f"峰值 {peak['cnt']:,.0f} 事件 @ {int(peak['hour']):02d}时 UTC ｜ "
               f"谷值 {low['cnt']:,.0f} @ {int(low['hour']):02d}时 ｜ "
               f"峰谷比 {peak['cnt'] / max(low['cnt'], 1):.1f}x")

# ──────────────────────────── 6. PR 漏斗 ────────────────────────────
st.subheader("PR 漏斗")
pr = q(f"""
    SELECT date, sum(opened_cnt) AS 新建, sum(merged_cnt) AS 已合并,
           sum(closed_cnt) AS 仅关闭,
           round(sum(merged_cnt) * 1.0 / nullif(sum(merged_cnt) + sum(closed_cnt), 0), 4) AS 合并率
    FROM ads.daily_pr_funnel WHERE date BETWEEN DATE '{d0}' AND DATE '{d1}'
    GROUP BY 1 ORDER BY 1
""")
left, right = st.columns([3, 2])
with left:
    melt = pr.melt(id_vars="date", value_vars=["新建", "已合并", "仅关闭"],
                   var_name="动作", value_name="数量")
    fig = px.line(melt, x="date", y="数量", color="动作", markers=True,
                  labels={"date": "日期"})
    st.plotly_chart(style(fig), use_container_width=True)
with right:
    fig = px.line(pr, x="date", y="合并率", markers=True, labels={"date": "日期"})
    fig.update_yaxes(tickformat=".0%")
    st.plotly_chart(style(fig), use_container_width=True)

st.caption(
    "合并率 = 已合并 / (已合并 + 仅关闭)。注意上游 GitHub Archive 裁剪了 "
    "`pull_request.merged` 字段（非空率 0%），这里的合并信号取自 `payload.action == 'merged'`。"
)

st.divider()
st.caption(f"warehouse: {C.WAREHOUSE_DB} ｜ 生成命令 `python scripts/build_duckdb.py`")
