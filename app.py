import streamlit as st
import pandas as pd
import os
import re
import datetime
from groq import Groq
from io import BytesIO

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(page_title="Enterprise AI Platform", layout="wide")

# -----------------------------------
# ENTERPRISE THEME
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
<h1>Enterprise AI Transformation & Delivery Platform</h1>
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

# -----------------------------------
# SAFE EXECUTION FUNCTION (Multi-DF)
# -----------------------------------
def safe_exec_multi(dataframes: dict, code: str):
    """
    Executes AI-generated code safely with multiple DataFrames.
    dataframes: dict of {alias: df}, e.g. {"df1": df1, "df2": df2, "df": df1}
    Returns the resulting 'df' from local_env after execution.
    """
    # Strip markdown fences
    code = re.sub(r"```(?:python)?", "", code)
    code = re.sub(r"```", "", code)
    code = code.strip()

    local_env = {**dataframes, "pd": pd}
    try:
        exec(code, {}, local_env)
    except Exception as e:
        st.warning(f"Failed executing AI code: {e}")
        # Return the primary df unchanged
        return dataframes.get("df", list(dataframes.values())[0])

    # Prefer 'result' then 'df', else first df value
    return local_env.get("result", local_env.get("df", list(dataframes.values())[0]))


# -----------------------------------
# BUILD SYSTEM PROMPT FOR ETL
# -----------------------------------
def build_system_prompt(dataframes: dict) -> str:
    """Build a rich system prompt showing all uploaded dataframe schemas."""
    schema_info = ""
    for alias, df in dataframes.items():
        schema_info += f"\n  {alias}: columns={df.columns.tolist()}, shape={df.shape}"

    aliases = list(dataframes.keys())
    primary = aliases[0] if aliases else "df"

    join_examples = ""
    if len(aliases) >= 2:
        a, b = aliases[0], aliases[1]
        # Find common columns for example
        common = list(set(dataframes[a].columns) & set(dataframes[b].columns))
        join_col = common[0] if common else "id"
        join_examples = f"""
# Example: Inner join {a} and {b} on '{join_col}'
result = pd.merge({a}, {b}, on='{join_col}', how='inner')

# Example: Left join
result = pd.merge({a}, {b}, on='{join_col}', how='left')

# Example: Join on different column names
result = pd.merge({a}, {b}, left_on='emp_id', right_on='employee_id', how='inner')

# Example: Multi-key join
result = pd.merge({a}, {b}, on=['{join_col}'], how='inner')
"""

    return f"""
You are a Senior Enterprise Data Engineer. Follow these rules STRICTLY:

AVAILABLE DATAFRAMES:{schema_info}

PRIMARY DATAFRAME: '{primary}' (use as default if no join is needed)

RULES:
- Use ONLY the available dataframe aliases above.
- For single-file operations, modify '{primary}' and store result in 'result' or '{primary}'.
- For joins/merges, use pd.merge() and store result in 'result'.
- Handle nulls using fillna("").
- Strip spaces using .str.strip().
- Compare strings using .str.lower().
- Use vectorized pandas operations only — no row loops.
- Do NOT include explanations, markdown fences, or comments.
- Return ONLY executable Python code.
- Apply exact filters requested (e.g., Salary > 75000, Department = IT).
- Ensure numeric filters and string equality are applied correctly.
- Always assign final output to 'result'.

FEW-SHOT EXAMPLES:

# Example 1 — Filter single file:
result = {primary}[{primary}['Salary'] > 70000]

# Example 2 — String filter:
result = {primary}[{primary}['Department'].str.strip().str.lower() == "it"]

# Example 3 — Combined filter:
result = {primary}[({primary}['Salary'] >= 80000) & ({primary}['Department'].str.strip().str.lower() == "finance")]
{join_examples}
# Example 4 — Add computed column then filter:
df_temp = {primary}.copy()
df_temp['FullName'] = df_temp['FirstName'].str.strip() + ' ' + df_temp['LastName'].str.strip()
result = df_temp[df_temp['Salary'] > 60000]
"""


# -----------------------------------
# TABS
# -----------------------------------
tab1, tab2 = st.tabs(["AI ETL Engine", "AI Jira Breakdown"])

# ===================================
# ========== AI ETL TAB =============
# ===================================
with tab1:
    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    etl_prompt = st.text_area("Describe data transformation", key="etl_prompt", height=140)

    uploaded_files = st.file_uploader(
        "Upload CSV File(s) — upload multiple files to enable joins",
        type=["csv"],
        accept_multiple_files=True,
        key="etl_upload"
    )

    # Preview uploaded files and show schema info
    if uploaded_files:
        st.markdown('<div class="section-title">Uploaded Files Preview</div>', unsafe_allow_html=True)
        dataframes = {}
        for i, f in enumerate(uploaded_files):
            alias = f"df{i+1}" if len(uploaded_files) > 1 else "df"
            df_temp = pd.read_csv(f)
            dataframes[alias] = df_temp
            with st.expander(f"📄 {f.name}  →  alias: `{alias}`  |  {df_temp.shape[0]} rows × {df_temp.shape[1]} cols"):
                st.dataframe(df_temp.head(5), use_container_width=True)

        if len(uploaded_files) > 1:
            st.info(
                f"**{len(uploaded_files)} files loaded.** "
                f"Reference them as: {', '.join(f'`{a}`' for a in dataframes)}. "
                "You can describe joins in plain English, e.g. *'Join df1 and df2 on EmployeeID and filter Salary > 70000'*."
            )

    if st.button("Execute ETL"):
        if not etl_prompt.strip():
            st.warning("Enter transformation description.")
            st.stop()
        if not uploaded_files:
            st.warning("Upload at least one CSV file.")
            st.stop()

        # Build dataframes dict
        dataframes = {}
        for i, f in enumerate(uploaded_files):
            f.seek(0)
            alias = f"df{i+1}" if len(uploaded_files) > 1 else "df"
            dataframes[alias] = pd.read_csv(f)

        # For single file keep backward compat alias 'df'
        if len(uploaded_files) == 1:
            dataframes["df"] = list(dataframes.values())[0]

        primary_alias = "df" if len(uploaded_files) == 1 else "df1"
        original_rows = dataframes.get(primary_alias, list(dataframes.values())[0]).shape[0]

        system_prompt = build_system_prompt(dataframes)

        def generate_code(error=None):
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": etl_prompt}
            ]
            if error:
                messages.append({
                    "role": "user",
                    "content": (
                        f"The previous code caused this error: {error}\n"
                        "Please fix the code. Return only executable Python. "
                        "Store final result in 'result'."
                    )
                })
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=messages,
                temperature=0.1
            )
            return response.choices[0].message.content

        ai_code = ""
        transformed_df = None
        try:
            ai_code = generate_code()
            transformed_df = safe_exec_multi(dataframes, ai_code)
        except Exception as e:
            st.warning(f"First attempt failed ({e}), retrying with error context...")
            try:
                ai_code = generate_code(error=str(e))
                transformed_df = safe_exec_multi(dataframes, ai_code)
            except Exception as e2:
                st.error(f"ETL failed after retry: {e2}")
                transformed_df = list(dataframes.values())[0].copy()

        st.subheader("Generated Code")
        st.code(ai_code, language="python")

        st.subheader("Transformed Output")
        st.dataframe(transformed_df, use_container_width=True)

        # Save history
        st.session_state.history.append({
            "Time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Prompt": etl_prompt,
            "Files": ", ".join(f.name for f in uploaded_files),
            "Rows Before": original_rows,
            "Rows After": len(transformed_df)
        })

        # Export
        st.markdown('<div class="section-title">Export Results</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        csv_data = transformed_df.to_csv(index=False).encode("utf-8")
        col1.download_button("Download CSV", csv_data, "etl_output.csv", "text/csv")
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            transformed_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            pd.DataFrame(st.session_state.history).to_excel(writer, sheet_name="Audit_Log", index=False)
        col2.download_button(
            "Download Excel", output.getvalue(), "etl_output.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ===================================
# ========== JIRA TAB ==============
# ===================================
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
                    {"role": "user", "content": jira_prompt}
                ],
                temperature=0.3
            )
            jira_output = response.choices[0].message.content

        st.subheader("Jira Breakdown")
        st.markdown(jira_output)
        st.markdown('<div class="section-title">Export Jira Output</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        col1.download_button("Download as TXT", jira_output, "jira_breakdown.txt", "text/plain")
        jira_df = pd.DataFrame({"Jira Breakdown": [jira_output]})
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            jira_df.to_excel(writer, sheet_name="Jira_Output", index=False)
        col2.download_button(
            "Download as Excel", output.getvalue(), "jira_breakdown.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ===================================
# HISTORY PANEL
# ===================================
st.markdown("---")
st.markdown('<div class="section-title">Transformation History</div>', unsafe_allow_html=True)
if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history))
else:
    st.info("No transformations executed yet.")
