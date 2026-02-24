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
# ENTERPRISE THEME + Switch CSS
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

/* Switch styling */
.switch {
  position: relative;
  display: inline-block;
  width: 60px;
  height: 28px;
  margin-top: 10px;
}
.switch input { opacity: 0; width: 0; height: 0; }
.slider {
  position: absolute;
  cursor: pointer;
  background-color: #ccc;
  border-radius: 34px;
  top: 0; left: 0; right: 0; bottom: 0;
  transition: .4s;
}
.slider:before {
  position: absolute;
  content: "";
  height: 22px;
  width: 22px;
  left: 3px;
  bottom: 3px;
  background-color: white;
  border-radius: 50%;
  transition: .4s;
}
input:checked + .slider { background-color: #B31B1B; }
input:checked + .slider:before { transform: translateX(32px); }
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
# CHECK SPARK AVAILABILITY
# -----------------------------------
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, trim, lit
    spark_available = True
except (ModuleNotFoundError, Exception):
    spark_available = False

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

    # ---------------------------
    # Spark toggle
    # ---------------------------
    st.markdown('<div class="section-title">ETL Engine Selection</div>', unsafe_allow_html=True)
    if spark_available:
        use_spark = st.checkbox("Enable Spark Engine for Large Datasets", value=False, key="spark_toggle_hidden")
    else:
        use_spark = False
        st.info("PySpark not available: Spark Engine disabled.")

    if st.button("Execute ETL"):
        if not etl_prompt.strip():
            st.warning("Enter transformation description.")
            st.stop()
        if not uploaded_file:
            st.warning("Upload CSV file.")
            st.stop()

        # -----------------------------------
        # PANDAS ETL
        # -----------------------------------
        if not use_spark:
            df = pd.read_csv(uploaded_file)
            original_rows = len(df)

            with st.spinner("Generating enterprise transformation..."):
                system_prompt = f"""
You are a Senior Enterprise Data Engineer.

STRICT RULES:
- DataFrame name is df
- Handle nulls using fillna("")
- Strip spaces using .str.strip()
- Compare strings using .str.lower()
- No inplace=True
- No loops for filtering
- Use vectorized pandas
- Return ONLY executable Python code
- Columns available: {df.columns.tolist()}
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
                    code = response.choices[0].message.content

                    # ----------------
                    # SANITIZE CODE
                    # ----------------
                    code = re.sub(r"```python", "", code, flags=re.IGNORECASE)
                    code = re.sub(r"```", "", code)
                    code_lines = code.splitlines()
                    code_lines = [line for line in code_lines if not line.strip().startswith("This line of code")]
                    return "\n".join(code_lines).strip()

                try:
                    code = generate_code()
                    banned = ["import os", "import sys", "subprocess", "eval(", "exec(", "open("]
                    if any(b in code for b in banned):
                        st.error("Unsafe code detected.")
                        st.stop()

                    local_env = {"df": df.copy(), "pd": pd}
                    exec(code, {}, local_env)
                    transformed_df = local_env["df"]

                except SyntaxError as se:
                    st.error(f"Syntax error in generated code: {se}")
                    st.stop()

            st.subheader("Generated Code")
            st.code(code)
            st.subheader("Transformed Output")
            st.dataframe(transformed_df, use_container_width=True)

        # -----------------------------------
        # SPARK ETL
        # -----------------------------------
        else:
            try:
                spark = SparkSession.builder.appName("EnterpriseETL").getOrCreate()
                df_spark = spark.read.csv(uploaded_file, header=True, inferSchema=True)
                original_rows = df_spark.count()

                with st.spinner("Generating Spark transformation..."):
                    columns_list = df_spark.columns
                    system_prompt_spark = f"""
You are a Senior Enterprise Data Engineer.

STRICT RULES:
- DataFrame name is df_spark
- Use PySpark DataFrame operations only
- Handle nulls using fillna("")
- Strip string columns using trim()
- Return ONLY executable PySpark Python code
- Columns available: {columns_list}
"""
                    messages = [
                        {"role": "system", "content": system_prompt_spark},
                        {"role": "user", "content": etl_prompt}
                    ]
                    response = client.chat.completions.create(
                        model="llama-3.1-8b-instant",
                        messages=messages,
                        temperature=0.1
                    )
                    code = response.choices[0].message.content
                    code = re.sub(r"```python", "", code)
                    code = re.sub(r"```", "", code).strip()

                    local_env = {"df_spark": df_spark, "spark": spark, "col": col, "trim": trim, "lit": lit}
                    exec(code, {}, local_env)
                    transformed_df_spark = local_env["df_spark"]

                st.subheader("Generated Spark Code")
                st.code(code)
                st.subheader("Transformed Spark Output")
                st.dataframe(transformed_df_spark.toPandas(), use_container_width=True)

            except Exception as e:
                st.error(f"Spark ETL failed: {e}")
                st.stop()

        # -----------------------------------
        # SAVE HISTORY
        # -----------------------------------
        rows_after = len(transformed_df) if not use_spark else transformed_df_spark.count()
        st.session_state.history.append({
            "Time": datetime.datetime.now(),
            "Prompt": etl_prompt,
            "Engine": "Spark" if use_spark else "Pandas",
            "Rows Before": original_rows,
            "Rows After": rows_after
        })

        # -----------------------------------
        # EXPORT RESULTS
        # -----------------------------------
        st.markdown('<div class="section-title">Export Results</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        output_df = transformed_df if not use_spark else transformed_df_spark.toPandas()
        csv_data = output_df.to_csv(index=False).encode("utf-8")
        col1.download_button("Download CSV", csv_data, "etl_output.csv", "text/csv")
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            output_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            pd.DataFrame(st.session_state.history).to_excel(writer, sheet_name="Audit_Log", index=False)
        col2.download_button(
            "Download Excel",
            output.getvalue(),
            "etl_output.xlsx",
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
            "Download as Excel",
            output.getvalue(),
            "jira_breakdown.xlsx",
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
