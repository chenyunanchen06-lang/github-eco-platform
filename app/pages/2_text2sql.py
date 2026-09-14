"""
09 · 智能问数界面（Streamlit 子页面）

由 dashboard.py 的多页机制自动挂载：streamlit run app/dashboard.py 后，
左侧会出现「2_text2sql」入口。

不配置 API Key 也能用 —— 会退化为模板模式，用预置 SQL 回答常见问题。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from text2sql import agent

st.set_page_config(page_title="智能问数", page_icon="💬", layout="wide")
st.title("智能问数")
st.caption("用中文提问，自动生成 SQL 并查询 warehouse.duckdb")

EXAMPLES = [
    "最近一周最活跃的仓库有哪些",
    "机器人账号占多少比例",
    "每小时什么时段最活跃",
    "PR 合并率怎么样",
    "最活跃的组织 Top10",
    "最活跃的开发者是谁",
]

# ─────────────────────────── 侧边栏 ───────────────────────────
with st.sidebar:
    st.subheader("模型配置")
    if agent.llm_available():
        st.success("已启用 LLM 模式")
    else:
        st.warning("未配置 API Key，当前为**模板模式**（仅能回答预置问题）")
        key = st.text_input("临时填入 API Key", type="password",
                            help="只存在当前会话内存里，不写入任何文件")
        if key:
            os.environ["TEXT2SQL_API_KEY"] = key.strip()
            st.rerun()
    with st.expander("换成别的模型"):
        st.caption(
            "支持任何 OpenAI 兼容接口，用环境变量指定：\n\n"
            "```\nTEXT2SQL_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1\n"
            "TEXT2SQL_MODEL=qwen-plus\nTEXT2SQL_API_KEY=sk-xxx\n```\n\n"
            "Ollama 本地模型把 BASE_URL 设成 `http://localhost:11434/v1`、"
            "MODEL 设成模型名即可（Key 随便填）。\n\n"
            "⚠️ 本地 4B 级别的小模型做 Text-to-SQL 基本不够用，会编造列名。"
        )

# ─────────────────────────── 示例问题 ───────────────────────────
st.write("**试试这些问题：**")
cols = st.columns(3)
for i, ex in enumerate(EXAMPLES):
    if cols[i % 3].button(ex, use_container_width=True):
        st.session_state["question"] = ex

question = st.text_input("你的问题", value=st.session_state.get("question", ""),
                         placeholder="例如：最近一周最活跃的仓库有哪些")
go = st.button("查询", type="primary")

# ─────────────────────────── 执行 ───────────────────────────
if go and question.strip():
    with st.spinner("生成 SQL 中…"):
        result = agent.ask(question.strip())

    badge = {"llm": "🟢 LLM 生成", "template": "🟡 模板匹配", "refused": "🔴 未生成"}
    st.markdown(f"**状态**：{badge.get(result.mode, result.mode)} ｜ "
                f"耗时 {result.elapsed:.2f}s")

    if result.error and not result.rows:
        st.error(result.error)
    else:
        st.markdown("**生成的 SQL**")
        st.code(result.sql, language="sql")

        if result.rows:
            df = pd.DataFrame(result.rows, columns=result.columns)
            st.markdown(f"**结果**（{len(df)} 行）")
            st.dataframe(df, use_container_width=True)

    if result.attempts:
        with st.expander(f"执行过程（{len(result.attempts)} 轮）"):
            for a in result.attempts:
                icon = "✅" if a["ok"] else "❌"
                st.markdown(f"{icon} **第 {a['round']} 轮** —— {a['note']}")
                if a.get("sql"):
                    st.code(a["sql"], language="sql")

st.divider()
with st.expander("它是怎么工作的"):
    st.markdown("""
1. 把 `warehouse.duckdb` 里的视图结构（表名 + 字段 + 语义说明）拼进系统提示词
2. 让模型生成**一条** SQL，剥掉可能夹带的 markdown 围栏和解释文字
3. 安全校验：只允许 `SELECT` / `WITH`，出现任何写操作关键字直接拒绝
4. 执行 SQL；**如果报错，把数据库的原始错误信息回灌给模型重试**（最多 3 轮）
5. 3 轮仍失败 → 拒答，不编造结果

第 4 步是关键：模型第一次经常写错列名，但看到
`Binder Error: Referenced column "xxx" not found` 之后基本能自己改对。
""")
