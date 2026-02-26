import streamlit as st
import pandas as pd
import numpy as np
import os
import re
import math
import string
import datetime
import json
import time
from groq import Groq
from io import BytesIO

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(page_title="Enterprise AI Platform", layout="wide")

# -----------------------------------
# FULL CSS
# -----------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Rajdhani:wght@600;700&display=swap');

* { font-family: 'Inter', sans-serif; }

/* ── BUILT BY BANNER ── */
.built-by-banner {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 8px;
    padding: 6px 16px 0 0;
    margin-bottom: -6px;
}
.built-by-banner .byline {
    font-size: 11px;
    color: #999;
    letter-spacing: 0.8px;
    text-transform: uppercase;
}
.built-by-banner .author {
    font-family: 'Rajdhani', sans-serif;
    font-size: 15px;
    font-weight: 700;
    color: #B31B1B;
    letter-spacing: 1px;
}
.built-by-banner .dot {
    width: 6px; height: 6px;
    background: #FFC72C;
    border-radius: 50%;
    display: inline-block;
}

/* ── MAIN HEADER ── */
.main-header {
    background: linear-gradient(135deg, #B31B1B 0%, #7a1212 100%);
    padding: 22px 28px;
    border-radius: 10px;
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 4px 15px rgba(179,27,27,0.3);
}
.main-header h1 {
    color: #FFC72C;
    margin: 0;
    font-family: 'Rajdhani', sans-serif;
    font-size: 28px;
    font-weight: 700;
    letter-spacing: 1px;
}
.main-header .header-sub {
    color: rgba(255,255,255,0.6);
    font-size: 12px;
    margin-top: 4px;
    letter-spacing: 0.5px;
}
.main-header .version-badge {
    background: rgba(255,199,44,0.15);
    border: 1px solid rgba(255,199,44,0.4);
    color: #FFC72C;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 1px;
}

/* ── SECTION TITLE ── */
.section-title {
    color: #B31B1B;
    font-weight: 600;
    font-size: 16px;
    margin-top: 20px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

/* ── BUTTONS ── */
.stButton>button {
    background-color: #B31B1B;
    color: white;
    font-weight: bold;
    border-radius: 6px;
}
.stButton>button:hover {
    background-color: #8E1414;
    color: #FFC72C;
}

/* ── METRICS ROW ── */
.metric-row {
    display: flex;
    gap: 12px;
    margin: 16px 0 8px 0;
    flex-wrap: wrap;
}
.metric-box {
    background: white;
    border: 1px solid #E8E8E8;
    border-top: 3px solid #B31B1B;
    border-radius: 8px;
    padding: 14px 18px;
    min-width: 120px;
    text-align: center;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    flex: 1;
}
.metric-box .metric-value {
    font-size: 28px;
    font-weight: 700;
    color: #B31B1B;
    font-family: 'Rajdhani', sans-serif;
}
.metric-box .metric-label {
    font-size: 10px;
    color: #999;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-top: 2px;
}

/* ── PIPELINE LOG STEPS (inside expander) ── */
.pipeline-steps {
    padding: 4px 0;
}
.pipeline-step {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 10px 0;
    border-bottom: 1px solid #F0F0F0;
    font-size: 14px;
    color: #333;
}
.pipeline-step:last-child { border-bottom: none; }
.step-num {
    background: #B31B1B;
    color: white;
    border-radius: 50%;
    width: 22px; height: 22px;
    display: flex; align-items: center; justify-content: center;
    font-size: 11px; font-weight: 700;
    min-width: 22px;
}
.step-icon { font-size: 18px; min-width: 24px; }
.step-text { flex: 1; line-height: 1.5; }
.badge-ok {
    background: #E8F5E9; color: #2E7D32;
    border-radius: 10px; padding: 1px 9px;
    font-size: 11px; font-weight: 600;
}

/* ── EXECUTION FLOW (GDE style) ── */
.gde-container {
    background: #0D1117;
    border-radius: 10px;
    padding: 24px 20px;
    margin: 12px 0;
    overflow-x: auto;
    box-shadow: inset 0 2px 8px rgba(0,0,0,0.4);
}
.gde-flow {
    display: flex;
    align-items: center;
    gap: 0;
    min-width: max-content;
    padding: 8px 0;
}
.gde-node {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 6px;
}
.gde-node-box {
    border-radius: 8px;
    padding: 10px 16px;
    text-align: center;
    min-width: 110px;
    cursor: default;
    transition: all 0.3s ease;
    position: relative;
}
/* Input node */
.gde-node-box.input {
    background: #1a2744;
    border: 2px solid #1E90FF;
    color: #7BB8FF;
}
/* Transform node */
.gde-node-box.transform {
    background: #1a2a1a;
    border: 2px solid #00C853;
    color: #69F0AE;
}
.gde-node-box.transform.running {
    background: #1a2a1a;
    border: 2px solid #FFD600;
    color: #FFD600;
    box-shadow: 0 0 12px rgba(255,214,0,0.4);
    animation: pulse 1s infinite;
}
.gde-node-box.transform.done {
    background: #0d2137;
    border: 2px solid #29B6F6;
    color: #29B6F6;
    box-shadow: 0 0 10px rgba(41,182,246,0.3);
}
/* Output node */
.gde-node-box.output {
    background: #1a1a2e;
    border: 2px solid #AB47BC;
    color: #CE93D8;
}
.gde-node-box.output.done {
    background: #0d2137;
    border: 2px solid #29B6F6;
    color: #29B6F6;
    box-shadow: 0 0 10px rgba(41,182,246,0.3);
}
@keyframes pulse {
    0%,100% { box-shadow: 0 0 8px rgba(255,214,0,0.3); }
    50%      { box-shadow: 0 0 20px rgba(255,214,0,0.7); }
}
.gde-node-title {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.8px;
    text-transform: uppercase;
}
.gde-node-sub {
    font-size: 10px;
    opacity: 0.7;
    margin-top: 2px;
}
.gde-node-count {
    font-size: 13px;
    font-weight: 700;
    font-family: 'Rajdhani', sans-serif;
    margin-top: 4px;
}
.gde-node-label {
    font-size: 10px;
    color: #666;
    text-align: center;
    max-width: 110px;
}
/* Connector arrow */
.gde-arrow {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 4px;
    padding: 0 6px;
    min-width: 70px;
}
.gde-count-label {
    font-size: 10px;
    color: #666;
    white-space: nowrap;
    text-align: center;
}
/* Legend */
.gde-legend {
    display: flex;
    gap: 20px;
    margin-top: 16px;
    flex-wrap: wrap;
}
.gde-legend-item {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: #888;
}
.legend-dot {
    width: 10px; height: 10px;
    border-radius: 2px;
}
</style>
""", unsafe_allow_html=True)

# ── BUILT BY BANNER ──
st.markdown("""
<div class="built-by-banner">
    <span class="byline">Built by</span>
    <span class="dot"></span>
    <span class="author">PRADEEP</span>
    <span class="dot"></span>
    <span class="byline">Enterprise AI</span>
</div>
""", unsafe_allow_html=True)

# ── MAIN HEADER ──
st.markdown("""
<div class="main-header">
    <div>
        <div class="header-sub">AI POWERED &nbsp;·&nbsp; LLAMA 3.3 &nbsp;·&nbsp; PANDAS</div>
        <h1>⚡ Enterprise AI Transformation &amp; Delivery Platform</h1>
    </div>
    <div class="version-badge">v2.0</div>
</div>
""", unsafe_allow_html=True)

# -----------------------------------
# GROQ SETUP
# -----------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    st.error("Set GROQ_API_KEY in Streamlit Secrets.")
    st.stop()
client = Groq(api_key=GROQ_API_KEY)

if "history" not in st.session_state:
    st.session_state.history = []


# ============================================================
# UTILITY
# ============================================================
def extract_code(raw: str) -> str:
    fenced = re.search(r"```(?:python)?\s*\n(.*?)```", raw, re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("```")]
    return "\n".join(lines).strip()


# ============================================================
# GDE EXECUTION FLOW  (Abinitio-style)
# ============================================================
def render_gde_flow(dataframes: dict, file_names: list, code: str,
                    result_df: pd.DataFrame, state: str = "done"):
    """
    Renders an Abinitio GDE-style execution flow:
      INPUT1 ──►  JOIN/TRANSFORM  ──►  OUTPUT
    state: 'running' (yellow/pulse) or 'done' (blue/complete)
    """
    aliases = list(dataframes.keys())
    real_aliases = [a for a in aliases if a != "df"]
    if not real_aliases:
        real_aliases = ["df"]

    has_join = len(real_aliases) >= 2
    node_state = state  # 'running' or 'done'

    # Detect transform types from code
    code_lower = code.lower()
    trans_ops = []
    if "merge" in code_lower or "join" in code_lower: trans_ops.append("JOIN")
    if "groupby" in code_lower and "rank" in code_lower: trans_ops.append("RANK")
    if "pd.cut" in code_lower or "pd.qcut" in code_lower: trans_ops.append("BUCKET")
    if "re.sub" in code_lower or "replace" in code_lower: trans_ops.append("CLEAN")
    if "fillna" in code_lower: trans_ops.append("FILLNA")
    if not trans_ops: trans_ops.append("TRANSFORM")
    trans_label = " · ".join(trans_ops[:3])

    # Build node HTML pieces
    def input_node(alias, fname, rows):
        return f"""
        <div class="gde-node">
            <div class="gde-node-box input">
                <div class="gde-node-title">📂 INPUT</div>
                <div class="gde-node-sub">{fname}</div>
                <div class="gde-node-count">{rows:,} rows</div>
            </div>
            <div class="gde-node-label">{alias}</div>
        </div>"""

    def arrow(label="", done=False):
        stroke = "#29B6F6" if done else "#00C853"
        return f"""
        <div class="gde-arrow">
            <svg width="70" height="18" viewBox="0 0 70 18" xmlns="http://www.w3.org/2000/svg">
                <defs>
                    <marker id="arrowhead{'_done' if done else '_run'}" markerWidth="6" markerHeight="6"
                        refX="5" refY="3" orient="auto">
                        <polygon points="0 0, 6 3, 0 6" fill="{stroke}" />
                    </marker>
                </defs>
                <line x1="2" y1="9" x2="62" y2="9"
                    stroke="{stroke}" stroke-width="2"
                    marker-end="url(#arrowhead{'_done' if done else '_run'})" />
            </svg>
            <div class="gde-count-label" style="color:{stroke};">{label}</div>
        </div>"""

    def transform_node(label, sub, rows_in, state):
        cls = "transform " + state
        row_txt = f"{rows_in:,} rec" if rows_in else ""
        return f"""
        <div class="gde-node">
            <div class="gde-node-box {cls}">
                <div class="gde-node-title">⚙ {label}</div>
                <div class="gde-node-sub">{sub}</div>
                <div class="gde-node-count">{row_txt}</div>
            </div>
            <div class="gde-node-label">{'🟡 RUNNING' if state=='running' else '🔵 COMPLETE'}</div>
        </div>"""

    def output_node(rows, cols, state):
        cls = "output " + state
        return f"""
        <div class="gde-node">
            <div class="gde-node-box {cls}">
                <div class="gde-node-title">💾 OUTPUT</div>
                <div class="gde-node-sub">{cols} columns</div>
                <div class="gde-node-count">{rows:,} rows</div>
            </div>
            <div class="gde-node-label">RESULT</div>
        </div>"""

    # Compute row counts
    primary_rows = dataframes[real_aliases[0]].shape[0]
    secondary_rows = dataframes[real_aliases[1]].shape[0] if len(real_aliases) > 1 else 0
    joined_rows = primary_rows  # before filter
    out_rows = len(result_df) if state == "done" else 0
    out_cols = len(result_df.columns) if state == "done" else 0

    fname1 = file_names[0] if len(file_names) > 0 else real_aliases[0]
    fname2 = file_names[1] if len(file_names) > 1 else (real_aliases[1] if len(real_aliases) > 1 else "")

    # Build flow HTML
    flow_html = '<div class="gde-flow">'

    if has_join:
        # Two inputs stacked vertically feeding into join
        flow_html += f"""
        <div class="gde-node">
            <div style="display:flex;flex-direction:column;gap:10px;">
                <div class="gde-node">
                    <div class="gde-node-box input">
                        <div class="gde-node-title">📂 INPUT 1</div>
                        <div class="gde-node-sub">{fname1}</div>
                        <div class="gde-node-count">{primary_rows:,} rows</div>
                    </div>
                    <div class="gde-node-label">{real_aliases[0]}</div>
                </div>
                <div class="gde-node">
                    <div class="gde-node-box input">
                        <div class="gde-node-title">📂 INPUT 2</div>
                        <div class="gde-node-sub">{fname2}</div>
                        <div class="gde-node-count">{secondary_rows:,} rows</div>
                    </div>
                    <div class="gde-node-label">{real_aliases[1]}</div>
                </div>
            </div>
        </div>"""
        flow_html += arrow(f"{primary_rows + secondary_rows:,} in", done=(state == "done"))
    else:
        flow_html += input_node(real_aliases[0], fname1, primary_rows)
        flow_html += arrow(f"{primary_rows:,} in", done=(state == "done"))

    # Transform node
    flow_html += transform_node(trans_label, "AI GENERATED", joined_rows, node_state)
    flow_html += arrow(f"{out_rows:,} out" if state == "done" else "...", done=(state == "done"))

    # Output node
    flow_html += output_node(out_rows, out_cols, node_state)
    flow_html += "</div>"

    # Legend
    legend_html = """
    <div class="gde-legend">
        <div class="gde-legend-item"><div class="legend-dot" style="background:#1E90FF;"></div> Input Source</div>
        <div class="gde-legend-item"><div class="legend-dot" style="background:#FFD600;"></div> Running</div>
        <div class="gde-legend-item"><div class="legend-dot" style="background:#29B6F6;"></div> Complete</div>
        <div class="gde-legend-item"><div class="legend-dot" style="background:#AB47BC;"></div> Output</div>
    </div>"""

    full_html = f'<div class="gde-container">{flow_html}{legend_html}</div>'
    return full_html


# ============================================================
# PIPELINE LOG BUILDER
# ============================================================
def build_pipeline_log(code, dataframes, result_df, file_names, original_rows, attempt):
    aliases = list(dataframes.keys())
    summary_prompt = f"""
You are a data pipeline narrator for a business audience (no technical background).
The following Python pandas code was executed:
```python
{code}
```
Input files: {file_names}
Rows before: {original_rows}, Rows after: {len(result_df)}
Columns in result: {result_df.columns.tolist()}

Describe EXACTLY what this pipeline did in 4-8 plain-English bullet steps.
Rules:
- NO Python, NO code, NO technical jargon.
- Each step starts with an action verb (Loaded, Joined, Cleaned, Computed, Filtered, Sorted, Selected).
- Be specific: mention actual column names and values.
- One sentence per step max.
- Return ONLY a JSON array of strings like: ["Step one text", "Step two text"]
- No markdown, no explanation outside the JSON array.
"""
    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": summary_prompt}],
            temperature=0.2,
        )
        raw = resp.choices[0].message.content.strip()
        arr_match = re.search(r"\[.*\]", raw, re.DOTALL)
        steps = json.loads(arr_match.group()) if arr_match else [raw]
    except Exception:
        steps = [
            f"Loaded {original_rows:,} rows from {', '.join(file_names)}",
            "Applied AI-generated transformations",
            f"Produced {len(result_df):,} rows × {len(result_df.columns)} columns",
        ]

    icon_map = {
        "load":"📂","read":"📂","join":"🔗","merge":"🔗","combined":"🔗",
        "clean":"🧹","strip":"🧹","remov":"🧹","replac":"🧹",
        "comput":"⚙️","calculat":"⚙️","creat":"⚙️","add":"⚙️","generat":"⚙️",
        "filter":"🔍","kept":"🔍","exclud":"🔍","select":"🔍",
        "sort":"↕️","order":"↕️","rank":"🏅","format":"✏️",
    }

    def pick_icon(t):
        tl = t.lower()
        for kw, icon in icon_map.items():
            if kw in tl: return icon
        return "✅"

    steps_html = '<div class="pipeline-steps">'
    for i, step in enumerate(steps, 1):
        icon = pick_icon(step)
        steps_html += f"""
        <div class="pipeline-step">
            <span class="step-num">{i}</span>
            <span class="step-icon">{icon}</span>
            <span class="step-text">{step}</span>
        </div>"""
    steps_html += "</div>"

    new_cols = [c for c in result_df.columns if c not in list(dataframes.values())[0].columns]
    n_joins = max(0, len([a for a in aliases if a not in ("df",)]) - 1)

    metrics_html = f"""
    <div class="metric-row">
        <div class="metric-box"><div class="metric-value">{len(file_names)}</div><div class="metric-label">Files</div></div>
        <div class="metric-box"><div class="metric-value">{original_rows:,}</div><div class="metric-label">Rows In</div></div>
        <div class="metric-box"><div class="metric-value">{len(result_df):,}</div><div class="metric-label">Rows Out</div></div>
        <div class="metric-box"><div class="metric-value">{len(result_df.columns)}</div><div class="metric-label">Columns</div></div>
        <div class="metric-box"><div class="metric-value">{len(new_cols)}</div><div class="metric-label">New Cols</div></div>
        <div class="metric-box"><div class="metric-value">{n_joins}</div><div class="metric-label">Joins</div></div>
    </div>"""

    return metrics_html, steps_html


# ============================================================
# EXECUTION ENGINE
# ============================================================
def safe_exec_multi(dataframes: dict, code: str) -> pd.DataFrame:
    code = extract_code(code)
    exec_globals = {**globals(), **dataframes}
    try:
        exec(compile(code, "<ai_etl>", "exec"), exec_globals)
    except Exception as exc:
        raise RuntimeError(f"Failed executing AI code: {exc}\n\nCode:\n{code}") from exc
    primary = "df" if "df" in dataframes else list(dataframes.keys())[0]
    output = exec_globals.get("result", exec_globals.get(primary, list(dataframes.values())[0]))
    if not isinstance(output, pd.DataFrame):
        raise RuntimeError(f"AI code produced {type(output).__name__} instead of a DataFrame.")
    return output


# ============================================================
# SYSTEM PROMPT BUILDER
# ============================================================
def build_system_prompt(dataframes: dict) -> str:
    schema_lines = ""
    for alias, df in dataframes.items():
        dtypes = {c: str(t) for c, t in df.dtypes.items()}
        schema_lines += f"\n  {alias}: columns={df.columns.tolist()}, dtypes={dtypes}, shape={df.shape}"

    aliases = list(dataframes.keys())
    primary = aliases[0] if aliases else "df"
    join_examples = ""
    if len(aliases) >= 2:
        a, b = aliases[0], aliases[1]
        common = list(set(dataframes[a].columns) & set(dataframes[b].columns))
        jcol = common[0] if common else "id"
        join_examples = f"""
result = pd.merge({a}, {b}, on='{jcol}', how='inner')
result = pd.merge({a}, {b}, on='{jcol}', how='left')
result = pd.merge({a}, {b}, left_on='emp_id', right_on='employee_id', how='inner')
"""

    return f"""You are a Senior Enterprise Data Engineer. Follow every rule below.
AVAILABLE DATAFRAMES:{schema_lines}
PRIMARY DATAFRAME: '{primary}'
RULES:
1. Use ONLY the dataframe aliases listed above.
2. Store the final output in a variable named 'result'.
3. You MAY write `import` statements for any standard Python module.
4. Handle nulls: .fillna("") for strings, .fillna(0) for numerics.
5. Strip whitespace: .str.strip() before string comparisons.
6. String equality: .str.strip().str.lower() == "value".
7. Use vectorised pandas operations — never iterate rows with loops.
8. Do NOT output any explanation, markdown fences, or comments.
9. Return ONLY executable Python code.
10. Apply exact filters from the prompt.
11. Cast numeric columns where needed: pd.to_numeric(..., errors='coerce').
FEW-SHOT EXAMPLES:
result = {primary}[{primary}['SALARY'] > 70000]
result = {primary}[{primary}['DEPARTMENT'].str.strip().str.lower() == "it"]
import re
df_tmp = {primary}.copy()
df_tmp['PHONE_CLEAN'] = df_tmp['PHONE_NUMBER'].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
result = df_tmp
df_tmp = {primary}.copy()
df_tmp['RANK'] = df_tmp.groupby('DEPARTMENT_ID')['SALARY'].rank(method='dense', ascending=False)
result = df_tmp
df_tmp = {primary}.copy()
df_tmp['GRADE'] = pd.cut(df_tmp['SALARY'], bins=[0,10000,20000,float('inf')], labels=['LOW','MEDIUM','HIGH'])
result = df_tmp
{join_examples}"""


# ============================================================
# TABS
# ============================================================
tab1, tab2 = st.tabs(["AI ETL Engine", "AI Jira Breakdown"])


# ============================================================
# TAB 1 — AI ETL ENGINE
# ============================================================
with tab1:
    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    etl_prompt = st.text_area("Describe data transformation", key="etl_prompt", height=140)

    uploaded_files = st.file_uploader(
        "Upload CSV File(s) — upload multiple files to enable joins",
        type=["csv"], accept_multiple_files=True, key="etl_upload",
    )

    if uploaded_files:
        st.markdown('<div class="section-title">Uploaded Files Preview</div>', unsafe_allow_html=True)
        _preview_dfs = {}
        for i, f in enumerate(uploaded_files):
            alias = f"df{i+1}" if len(uploaded_files) > 1 else "df"
            _df = pd.read_csv(f)
            _preview_dfs[alias] = _df
            with st.expander(f"📄 {f.name}  →  alias: `{alias}`  |  {_df.shape[0]:,} rows × {_df.shape[1]} cols"):
                st.dataframe(_df.head(5), use_container_width=True)
        if len(uploaded_files) > 1:
            st.info(f"**{len(uploaded_files)} files loaded.** Reference them as: {', '.join(f'`{a}`' for a in _preview_dfs)}.")

    if st.button("Execute ETL"):
        if not etl_prompt.strip():
            st.warning("Enter a transformation description.")
            st.stop()
        if not uploaded_files:
            st.warning("Upload at least one CSV file.")
            st.stop()

        dataframes = {}
        for i, f in enumerate(uploaded_files):
            f.seek(0)
            alias = f"df{i+1}" if len(uploaded_files) > 1 else "df"
            dataframes[alias] = pd.read_csv(f)
        if len(uploaded_files) == 1:
            dataframes["df"] = list(dataframes.values())[0]

        primary_alias = "df" if len(uploaded_files) == 1 else "df1"
        original_rows = dataframes[primary_alias].shape[0]
        system_prompt = build_system_prompt(dataframes)
        file_names = [f.name for f in uploaded_files]

        # ── SECTION: EXECUTION FLOW (GDE) ──
        st.markdown('<div class="section-title">⚡ Execution Flow</div>', unsafe_allow_html=True)
        gde_placeholder = st.empty()

        # Show RUNNING state first
        running_gde = render_gde_flow(
            dataframes, file_names, "",
            pd.DataFrame(), state="running"
        )
        gde_placeholder.markdown(running_gde, unsafe_allow_html=True)

        # ── 3-attempt self-healing loop ──
        MAX_ATTEMPTS = 3
        ai_code = ""
        transformed_df = None
        last_error = None
        successful_attempt = 1

        conversation = [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": etl_prompt},
        ]

        with st.spinner("⚙️ Running AI ETL pipeline..."):
            for attempt in range(1, MAX_ATTEMPTS + 1):
                if last_error and attempt > 1:
                    conversation.append({"role": "assistant", "content": ai_code})
                    conversation.append({
                        "role": "user",
                        "content": (
                            f"Attempt {attempt-1} raised:\n{last_error}\n\n"
                            "Fix: no markdown fences, store result in 'result', "
                            "don't redefine pd or dataframe aliases."
                        ),
                    })
                response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=conversation, temperature=0.1,
                )
                ai_code = response.choices[0].message.content
                try:
                    transformed_df = safe_exec_multi(dataframes, ai_code)
                    last_error = None
                    successful_attempt = attempt
                    break
                except Exception as exc:
                    last_error = str(exc)
                    if attempt == MAX_ATTEMPTS:
                        st.error(f"ETL failed after {MAX_ATTEMPTS} attempts.\n\n{exc}")
                        transformed_df = list(dataframes.values())[0].copy()

        # Update GDE to DONE (blue)
        done_gde = render_gde_flow(
            dataframes, file_names, extract_code(ai_code),
            transformed_df, state="done"
        )
        gde_placeholder.markdown(done_gde, unsafe_allow_html=True)

        # ── SECTION: PIPELINE SUMMARY ──
        st.markdown('<div class="section-title">📊 Pipeline Execution Summary</div>', unsafe_allow_html=True)

        with st.spinner("Generating pipeline summary..."):
            metrics_html, steps_html = build_pipeline_log(
                extract_code(ai_code), dataframes, transformed_df,
                file_names, original_rows, successful_attempt,
            )

        # Metrics boxes — always visible
        st.markdown(metrics_html, unsafe_allow_html=True)

        # Plain English steps — inside expander (collapsed by default)
        with st.expander("📋 View detailed pipeline steps", expanded=False):
            st.markdown(steps_html, unsafe_allow_html=True)

        # ── RESULTS ──
        st.markdown('<div class="section-title">Transformed Output</div>', unsafe_allow_html=True)
        total_rows = len(transformed_df)
        col_info, col_select = st.columns([3, 1])
        col_info.markdown(
            f"<span style='font-size:13px;color:#666;'>Total records: <b>{total_rows:,}</b></span>",
            unsafe_allow_html=True
        )
        display_options = [20, 50, 100, 500, 1000]
        if total_rows <= 50000:
            display_options.append(total_rows)
        display_options = sorted(set(n for n in display_options if n <= total_rows)) or [total_rows]
        display_n = col_select.selectbox(
            "Show rows",
            options=display_options,
            index=0,
            key="display_rows"
        )
        st.dataframe(transformed_df.head(display_n), use_container_width=True)

        # Audit log
        st.session_state.history.append({
            "Time":        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Prompt":      etl_prompt,
            "Files":       ", ".join(file_names),
            "Rows Before": original_rows,
            "Rows After":  len(transformed_df),
            "Status":      "OK" if last_error is None else "FAILED",
        })

        # ── EXPORT ──
        st.markdown('<div class="section-title">Export Results</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        csv_bytes = transformed_df.to_csv(index=False).encode("utf-8")
        col1.download_button("Download CSV", csv_bytes, "etl_output.csv", "text/csv")
        xlsx_buf = BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="xlsxwriter") as writer:
            transformed_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            pd.DataFrame(st.session_state.history).to_excel(writer, sheet_name="Audit_Log", index=False)
        col2.download_button(
            "Download Excel", xlsx_buf.getvalue(), "etl_output.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ============================================================
# TAB 2 — JIRA BREAKDOWN (unchanged)
# ============================================================
with tab2:
    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    jira_prompt = st.text_area("Describe feature or initiative", key="jira_prompt", height=140)

    if st.button("Generate Jira Breakdown"):
        if not jira_prompt.strip():
            st.warning("Enter business description.")
            st.stop()
        with st.spinner("Generating Agile breakdown..."):
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": "You are a Senior Agile Delivery Manager. Generate: 1 Epic, Multiple User Stories, Acceptance Criteria, Subtasks. Return structured professional format."},
                    {"role": "user", "content": jira_prompt},
                ],
                temperature=0.3,
            )
            jira_output = response.choices[0].message.content

        st.subheader("Jira Breakdown")
        st.markdown(jira_output)
        st.markdown('<div class="section-title">Export Jira Output</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        col1.download_button("Download as TXT", jira_output, "jira_breakdown.txt", "text/plain")
        jira_buf = BytesIO()
        with pd.ExcelWriter(jira_buf, engine="xlsxwriter") as writer:
            pd.DataFrame({"Jira Breakdown": [jira_output]}).to_excel(writer, sheet_name="Jira_Output", index=False)
        col2.download_button(
            "Download as Excel", jira_buf.getvalue(), "jira_breakdown.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ============================================================
# HISTORY
# ============================================================
st.markdown("---")
st.markdown('<div class="section-title">Transformation History</div>', unsafe_allow_html=True)
if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history), use_container_width=True)
else:
    st.info("No transformations executed yet.")
