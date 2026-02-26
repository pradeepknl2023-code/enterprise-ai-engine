import streamlit as st
import pandas as pd
import numpy as np
import os
import re
import math
import string
import datetime
from groq import Groq
from io import BytesIO

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(page_title="Enterprise AI Platform", layout="wide")

# -----------------------------------
# ENTERPRISE THEME  (unchanged)
# -----------------------------------
st.markdown("""
<style>
.main-header {
    background-color: #B31B1B;
    padding: 20px;
    border-radius: 8px;
    margin-bottom: 20px;
}
.main-header h1 {
    color: #FFC72C;
    margin: 0;
}
.section-title {
    color: #B31B1B;
    font-weight: 600;
    font-size: 20px;
    margin-top: 20px;
}
.stButton>button {
    background-color: #B31B1B;
    color: white;
    font-weight: bold;
}
.stButton>button:hover {
    background-color: #8E1414;
    color: #FFC72C;
}
/* Pipeline log card */
.pipeline-card {
    background: #FAFAFA;
    border-left: 4px solid #B31B1B;
    border-radius: 6px;
    padding: 16px 20px;
    margin-bottom: 12px;
}
.pipeline-card h4 {
    color: #B31B1B;
    margin: 0 0 10px 0;
    font-size: 15px;
    letter-spacing: 0.5px;
    text-transform: uppercase;
}
.pipeline-step {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 6px 0;
    border-bottom: 1px solid #EEE;
    font-size: 14px;
    color: #333;
}
.pipeline-step:last-child { border-bottom: none; }
.step-icon { font-size: 16px; min-width: 22px; }
.step-text { flex: 1; }
.step-badge {
    background: #B31B1B;
    color: white;
    border-radius: 10px;
    padding: 1px 8px;
    font-size: 11px;
    font-weight: 600;
    white-space: nowrap;
}
.step-badge-green {
    background: #2E7D32;
    color: white;
    border-radius: 10px;
    padding: 1px 8px;
    font-size: 11px;
    font-weight: 600;
}
/* Summary metrics row */
.metric-row {
    display: flex;
    gap: 16px;
    margin: 16px 0;
    flex-wrap: wrap;
}
.metric-box {
    background: white;
    border: 1px solid #DDD;
    border-radius: 8px;
    padding: 14px 20px;
    min-width: 130px;
    text-align: center;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
.metric-box .metric-value {
    font-size: 26px;
    font-weight: 700;
    color: #B31B1B;
}
.metric-box .metric-label {
    font-size: 11px;
    color: #888;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-top: 2px;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
<h1>Enterprise AI Transformation &amp; Delivery Platform</h1>
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

# -----------------------------------
# SESSION STATE
# -----------------------------------
if "history" not in st.session_state:
    st.session_state.history = []


# ============================================================
# UTILITY: strip markdown fences from AI-generated code
# ============================================================
def extract_code(raw: str) -> str:
    fenced = re.search(r"```(?:python)?\s*\n(.*?)```", raw, re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("```")]
    return "\n".join(lines).strip()


# ============================================================
# PIPELINE LOG BUILDER  — reads the executed code and produces
# human-readable step descriptions. No code shown to users.
# ============================================================
def build_pipeline_log(
    code: str,
    dataframes: dict,
    result_df: pd.DataFrame,
    file_names: list,
    original_rows: int,
    attempt: int,
) -> str:
    """
    Ask the AI to summarise what the ETL code did in plain English steps.
    Returns an HTML string rendered as a pipeline activity card.
    """
    aliases = list(dataframes.keys())

    # Ask model to narrate the steps
    summary_prompt = f"""
You are a data pipeline narrator for a business audience (no technical background).

The following Python pandas code was executed as part of an ETL pipeline:

```python
{code}
```

Available dataframes: {aliases}
Input file(s): {file_names}
Rows before: {original_rows}
Rows after: {len(result_df)}
Columns in result: {result_df.columns.tolist()}

Describe EXACTLY what this pipeline did in 4–8 plain-English bullet steps.
Rules:
- NO Python, NO code, NO technical jargon.
- Each step starts with an action verb (Loaded, Joined, Cleaned, Computed, Filtered, Sorted, Selected).
- Be specific: mention actual column names and values where relevant.
- Keep each step to one sentence max.
- Return ONLY a JSON array of strings, example:
["Loaded 26 employee records from employees.csv", "Joined with departments data on Department ID"]
- No markdown, no explanation outside the JSON array.
"""

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": summary_prompt}],
            temperature=0.2,
        )
        raw = resp.choices[0].message.content.strip()
        # Extract JSON array
        arr_match = re.search(r"\[.*\]", raw, re.DOTALL)
        if arr_match:
            import json
            steps = json.loads(arr_match.group())
        else:
            steps = [raw]
    except Exception:
        steps = [
            f"Loaded {original_rows:,} rows from {', '.join(file_names)}",
            "Applied AI-generated transformations",
            f"Produced {len(result_df):,} rows × {len(result_df.columns)} columns",
        ]

    # Step icons heuristic
    icon_map = {
        "load": "📂", "read": "📂",
        "join": "🔗", "merge": "🔗", "combined": "🔗",
        "clean": "🧹", "strip": "🧹", "remov": "🧹", "replac": "🧹",
        "comput": "⚙️", "calculat": "⚙️", "creat": "⚙️", "add": "⚙️", "generat": "⚙️",
        "filter": "🔍", "kept": "🔍", "exclud": "🔍", "select": "🔍",
        "sort": "↕️", "order": "↕️",
        "rank": "🏅",
        "format": "✏️",
        "export": "💾", "output": "💾",
    }

    def pick_icon(text):
        t = text.lower()
        for kw, icon in icon_map.items():
            if kw in t:
                return icon
        return "✅"

    # Build HTML
    steps_html = ""
    for i, step in enumerate(steps, 1):
        icon = pick_icon(step)
        steps_html += f"""
        <div class="pipeline-step">
            <span class="step-icon">{icon}</span>
            <span class="step-text"><strong>Step {i}:</strong> {step}</span>
        </div>"""

    status_badge = (
        '<span class="step-badge-green">✓ SUCCESS</span>'
        if attempt <= 3
        else '<span class="step-badge">⚠ RECOVERED</span>'
    )

    # Summary metrics row
    new_cols = [c for c in result_df.columns if c not in list(dataframes.values())[0].columns]
    n_joins = len([a for a in aliases if a != ("df" if "df" in aliases else aliases[0])])

    metrics_html = f"""
    <div class="metric-row">
        <div class="metric-box">
            <div class="metric-value">{len(file_names)}</div>
            <div class="metric-label">Files Processed</div>
        </div>
        <div class="metric-box">
            <div class="metric-value">{original_rows:,}</div>
            <div class="metric-label">Rows In</div>
        </div>
        <div class="metric-box">
            <div class="metric-value">{len(result_df):,}</div>
            <div class="metric-label">Rows Out</div>
        </div>
        <div class="metric-box">
            <div class="metric-value">{len(result_df.columns)}</div>
            <div class="metric-label">Columns</div>
        </div>
        <div class="metric-box">
            <div class="metric-value">{len(new_cols)}</div>
            <div class="metric-label">New Columns</div>
        </div>
        <div class="metric-box">
            <div class="metric-value">{n_joins}</div>
            <div class="metric-label">Joins Applied</div>
        </div>
    </div>"""

    html = f"""
    <div class="pipeline-card">
        <h4>⚡ Pipeline Execution Log &nbsp; {status_badge}</h4>
        {metrics_html}
        {steps_html}
    </div>"""

    return html


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
        schema_lines += (
            f"\n  {alias}: columns={df.columns.tolist()}, "
            f"dtypes={dtypes}, shape={df.shape}"
        )

    aliases = list(dataframes.keys())
    primary = aliases[0] if aliases else "df"

    join_examples = ""
    if len(aliases) >= 2:
        a, b = aliases[0], aliases[1]
        common = list(set(dataframes[a].columns) & set(dataframes[b].columns))
        jcol = common[0] if common else "id"
        join_examples = f"""
# Join examples ({a} + {b}):
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
3. You MAY write `import` statements for any standard Python module (re, math, string, etc.).
4. Handle nulls: .fillna("") for strings, .fillna(0) for numerics.
5. Strip whitespace: .str.strip() before string comparisons.
6. String equality: .str.strip().str.lower() == "value".
7. Use vectorised pandas operations — never iterate rows with loops.
8. Do NOT output any explanation, markdown fences, or comments.
9. Return ONLY executable Python code.
10. Apply exact filters from the prompt (Salary > 75000, Department = IT, etc.).
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
        type=["csv"],
        accept_multiple_files=True,
        key="etl_upload",
    )

    # ---- Live schema preview ----
    if uploaded_files:
        st.markdown('<div class="section-title">Uploaded Files Preview</div>', unsafe_allow_html=True)
        _preview_dfs = {}
        for i, f in enumerate(uploaded_files):
            alias = f"df{i+1}" if len(uploaded_files) > 1 else "df"
            _df = pd.read_csv(f)
            _preview_dfs[alias] = _df
            with st.expander(
                f"📄 {f.name}  →  alias: `{alias}`  |  "
                f"{_df.shape[0]:,} rows × {_df.shape[1]} cols"
            ):
                st.dataframe(_df.head(5), use_container_width=True)

        if len(uploaded_files) > 1:
            st.info(
                f"**{len(uploaded_files)} files loaded.** "
                f"Reference them as: {', '.join(f'`{a}`' for a in _preview_dfs)}. "
                "Describe joins in plain English — e.g. "
                "*'Using df1 (employees) and df2 (departments), show salary > 70000'*."
            )

    # ---- Execute button ----
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

        # ---- 3-attempt self-healing loop ----
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
                            f"Attempt {attempt - 1} raised this error:\n{last_error}\n\n"
                            "Fix the code. Rules: no markdown fences, "
                            "store final DataFrame in 'result', "
                            "do not redefine pd or dataframe aliases."
                        ),
                    })

                response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=conversation,
                    temperature=0.1,
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
                        st.error(f"ETL failed after {MAX_ATTEMPTS} attempts.\n\nLast error:\n{exc}")
                        transformed_df = list(dataframes.values())[0].copy()

        # ---- Pipeline Log (replaces raw code block) ----
        st.markdown('<div class="section-title">Pipeline Execution Summary</div>', unsafe_allow_html=True)
        with st.spinner("Generating pipeline summary..."):
            log_html = build_pipeline_log(
                code=extract_code(ai_code),
                dataframes=dataframes,
                result_df=transformed_df,
                file_names=file_names,
                original_rows=original_rows,
                attempt=successful_attempt,
            )
        st.markdown(log_html, unsafe_allow_html=True)

        # ---- Results ----
        st.markdown('<div class="section-title">Transformed Output</div>', unsafe_allow_html=True)
        st.dataframe(transformed_df, use_container_width=True)

        # Audit log
        st.session_state.history.append({
            "Time":        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Prompt":      etl_prompt,
            "Files":       ", ".join(file_names),
            "Rows Before": original_rows,
            "Rows After":  len(transformed_df),
            "Status":      "OK" if last_error is None else "FAILED",
        })

        # ---- Export ----
        st.markdown('<div class="section-title">Export Results</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        csv_bytes = transformed_df.to_csv(index=False).encode("utf-8")
        col1.download_button("Download CSV", csv_bytes, "etl_output.csv", "text/csv")

        xlsx_buf = BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="xlsxwriter") as writer:
            transformed_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            pd.DataFrame(st.session_state.history).to_excel(writer, sheet_name="Audit_Log", index=False)
        col2.download_button(
            "Download Excel",
            xlsx_buf.getvalue(),
            "etl_output.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ============================================================
# TAB 2 — AI JIRA BREAKDOWN  (completely unchanged)
# ============================================================
with tab2:
    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    jira_prompt = st.text_area("Describe feature or initiative", key="jira_prompt", height=140)

    if st.button("Generate Jira Breakdown"):
        if not jira_prompt.strip():
            st.warning("Enter business description.")
            st.stop()
        with st.spinner("Generating Agile breakdown..."):
            jira_system_prompt = """
You are a Senior Agile Delivery Manager.
Generate:
- 1 Epic
- Multiple User Stories
- Acceptance Criteria
- Subtasks
Return structured professional format.
"""
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": jira_system_prompt},
                    {"role": "user",   "content": jira_prompt},
                ],
                temperature=0.3,
            )
            jira_output = response.choices[0].message.content

        st.subheader("Jira Breakdown")
        st.markdown(jira_output)

        st.markdown('<div class="section-title">Export Jira Output</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        col1.download_button("Download as TXT", jira_output, "jira_breakdown.txt", "text/plain")

        jira_df = pd.DataFrame({"Jira Breakdown": [jira_output]})
        jira_buf = BytesIO()
        with pd.ExcelWriter(jira_buf, engine="xlsxwriter") as writer:
            jira_df.to_excel(writer, sheet_name="Jira_Output", index=False)
        col2.download_button(
            "Download as Excel",
            jira_buf.getvalue(),
            "jira_breakdown.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ============================================================
# HISTORY PANEL
# ============================================================
st.markdown("---")
st.markdown('<div class="section-title">Transformation History</div>', unsafe_allow_html=True)
if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history), use_container_width=True)
else:
    st.info("No transformations executed yet.")
