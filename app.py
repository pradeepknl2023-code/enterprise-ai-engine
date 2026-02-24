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
# SAFE EXECUTION FUNCTION
# -----------------------------------
def safe_exec(df, code):
    """Executes AI-generated code safely."""
    code = re.sub(r"```.*?```", "", code, flags=re.DOTALL)
    python_lines = []
    for line in code.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if re.match(r"^(df|pd|import|from|\w+.*=)", line):
            python_lines.append(line)
    cleaned_code = "\n".join(python_lines)
    if not cleaned_code:
        return df
    local_env = {"df": df.copy(), "pd": pd}
    try:
        exec(cleaned_code, {}, local_env)
    except Exception as e:
        st.warning(f"Failed executing AI code: {e}")
        return df
    return local_env["df"]

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
    uploaded_file = st.file_uploader("Upload CSV File", type=["csv"], key="etl_upload")

    if st.button("Execute ETL"):
        if not etl_prompt.strip():
            st.warning("Enter transformation description.")
            st.stop()
        if not uploaded_file:
            st.warning("Upload CSV file.")
            st.stop()

        df = pd.read_csv(uploaded_file)
        original_rows = len(df)

        # ---------------------------
        # AI-generated transformations
        # ---------------------------
        system_prompt = f"""
You are a Senior Enterprise Data Engineer. Follow these rules STRICTLY:

- DataFrame name is df.
- Handle nulls using fillna("").
- Strip spaces using .str.strip().
- Compare strings using .str.lower().
- Use vectorized pandas operations only.
- No loops for filtering.
- Do not include explanations, markdown, or comments.
- Return ONLY executable Python code that modifies df.
- Apply exact filters requested in the prompt (e.g., Salary > 75000, Department = IT).
- Ensure numeric filters and string equality are applied correctly.
- Columns available: {df.columns.tolist()}

FEW-SHOT EXAMPLES:

# Example 1:
# Prompt: "Salary > 70000"
# Code:
df = df[df['Salary'] > 70000]

# Example 2:
# Prompt: "Department = IT"
# Code:
df = df[df['Department'].str.strip().str.lower() == "it"]

# Example 3:
# Prompt: "Salary >= 80000 and Department = Finance"
# Code:
df = df[(df['Salary'] >= 80000) & (df['Department'].str.strip().str.lower() == "finance")]
"""

        def generate_code(error=None):
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": etl_prompt}
            ]
            if error:
                messages.append({"role": "user", "content": f"Fix error: {error}"})
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=messages,
                temperature=0.1
            )
            return response.choices[0].message.content

        try:
            ai_code = generate_code()
            transformed_df = safe_exec(df, ai_code)
        except Exception as e:
            st.error(f"ETL failed: {e}")
            transformed_df = df.copy()

        st.subheader("Generated Code")
        st.code(ai_code)

        st.subheader("Transformed Output")
        st.dataframe(transformed_df, use_container_width=True)

        # Save history
        st.session_state.history.append({
            "Time": datetime.datetime.now(),
            "Prompt": etl_prompt,
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
        col2.download_button("Download Excel", output.getvalue(), "etl_output.xlsx",
                             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

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
        col2.download_button("Download as Excel", output.getvalue(), "jira_breakdown.xlsx",
                             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ===================================
# HISTORY PANEL
# ===================================
st.markdown("---")
st.markdown('<div class="section-title">Transformation History</div>', unsafe_allow_html=True)
if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history))
else:
    st.info("No transformations executed yet.")
