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
    """
    Pull the Python code out of whatever the model returns.
    Handles ```python ... ```, ``` ... ```, and plain text.
    """
    fenced = re.search(r"```(?:python)?\s*\n(.*?)```", raw, re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("```")]
    return "\n".join(lines).strip()


# ============================================================
# EXECUTION ENGINE
# ============================================================
def safe_exec_multi(dataframes: dict, code: str) -> pd.DataFrame:
    """
    Execute AI-generated pandas code.

    Uses globals() as the execution namespace so every module already
    imported at the top of this file (pd, np, re, math, string, datetime)
    is automatically available — no more 'name X is not defined' errors.
    The AI can also use bare `import` statements for any installed package.
    Dataframe aliases (df / df1 / df2 ...) are overlaid on top.

    Output resolution: 'result' -> primary alias -> first dataframe.
    Raises RuntimeError on failure so the caller can retry.
    """
    code = extract_code(code)

    exec_globals = {
        **globals(),   # pd, np, re, math, string, datetime, BytesIO ...
        **dataframes,  # df  /  df1, df2, df3 ...
    }

    try:
        exec(compile(code, "<ai_etl>", "exec"), exec_globals)
    except Exception as exc:
        raise RuntimeError(f"Failed executing AI code: {exc}\n\nCode:\n{code}") from exc

    primary = "df" if "df" in dataframes else list(dataframes.keys())[0]
    output = exec_globals.get(
        "result",
        exec_globals.get(primary, list(dataframes.values())[0])
    )

    if not isinstance(output, pd.DataFrame):
        raise RuntimeError(
            f"AI code produced {type(output).__name__} instead of a DataFrame."
        )
    return output


# ============================================================
# SYSTEM PROMPT BUILDER
# ============================================================
def build_system_prompt(dataframes: dict) -> str:
    """Dynamically build a rich system prompt with schema + dtypes + join examples."""
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
result = pd.merge({a}, {b}, on=['{jcol}', 'DEPT'], how='inner')
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

# Filter by salary
result = {primary}[{primary}['SALARY'] > 70000]

# String filter
result = {primary}[{primary}['DEPARTMENT'].str.strip().str.lower() == "it"]

# Combined filter
result = {primary}[
    ({primary}['SALARY'] >= 80000) &
    ({primary}['DEPARTMENT'].str.strip().str.lower() == "finance")
]

# Regex phone cleaning
import re
df_tmp = {primary}.copy()
df_tmp['PHONE_CLEAN'] = df_tmp['PHONE_NUMBER'].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
result = df_tmp

# Dense rank within group
df_tmp = {primary}.copy()
df_tmp['RANK'] = df_tmp.groupby('DEPARTMENT_ID')['SALARY'].rank(method='dense', ascending=False)
result = df_tmp

# Salary grade buckets
df_tmp = {primary}.copy()
df_tmp['GRADE'] = pd.cut(
    df_tmp['SALARY'],
    bins=[0, 10000, 20000, float('inf')],
    labels=['LOW', 'MEDIUM', 'HIGH']
)
result = df_tmp
{join_examples}"""


# ============================================================
# TABS  (structure unchanged)
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
                "*'Join df1 and df2 on EMPLOYEE_ID and show employees with SALARY > 70000'*."
            )

    # ---- Execute button ----
    if st.button("Execute ETL"):
        if not etl_prompt.strip():
            st.warning("Enter a transformation description.")
            st.stop()
        if not uploaded_files:
            st.warning("Upload at least one CSV file.")
            st.stop()

        # Reload (seek to start after preview already read the files)
        dataframes = {}
        for i, f in enumerate(uploaded_files):
            f.seek(0)
            alias = f"df{i+1}" if len(uploaded_files) > 1 else "df"
            dataframes[alias] = pd.read_csv(f)

        # Single-file backward-compat: also expose as 'df'
        if len(uploaded_files) == 1:
            dataframes["df"] = list(dataframes.values())[0]

        primary_alias = "df" if len(uploaded_files) == 1 else "df1"
        original_rows = dataframes[primary_alias].shape[0]
        system_prompt = build_system_prompt(dataframes)

        # ---- 3-attempt self-healing execution loop ----
        MAX_ATTEMPTS = 3
        ai_code = ""
        transformed_df = None
        last_error = None

        # Running conversation — each retry appends previous code + error
        conversation = [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": etl_prompt},
        ]

        for attempt in range(1, MAX_ATTEMPTS + 1):
            if last_error and attempt > 1:
                conversation.append({"role": "assistant", "content": ai_code})
                conversation.append({
                    "role": "user",
                    "content": (
                        f"Attempt {attempt - 1} raised this error:\n{last_error}\n\n"
                        "Fix the code. Rules reminder:\n"
                        "  - NO markdown fences.\n"
                        "  - You MAY use `import` for any needed module.\n"
                        "  - Store final DataFrame in 'result'.\n"
                        "  - Do NOT redefine pd or the dataframe aliases."
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
                break  # success
            except Exception as exc:
                last_error = str(exc)
                if attempt < MAX_ATTEMPTS:
                    st.warning(f"⚠️ Attempt {attempt} failed — self-healing... (`{exc}`)")
                else:
                    st.error(f"ETL failed after {MAX_ATTEMPTS} attempts.\n\nLast error:\n{exc}")
                    transformed_df = list(dataframes.values())[0].copy()
        # ---- end retry loop ----

        st.subheader("Generated Code")
        st.code(extract_code(ai_code), language="python")

        st.subheader("Transformed Output")
        st.dataframe(transformed_df, use_container_width=True)

        # Audit log
        st.session_state.history.append({
            "Time":        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Prompt":      etl_prompt,
            "Files":       ", ".join(f.name for f in uploaded_files),
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
            pd.DataFrame(st.session_state.history).to_excel(
                writer, sheet_name="Audit_Log", index=False
            )
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
