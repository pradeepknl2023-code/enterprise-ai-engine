import streamlit as st
import pandas as pd
from io import BytesIO
from huggingface_hub import InferenceClient
import re

# ==========================================================
# PAGE CONFIG
# ==========================================================
st.set_page_config(
    page_title="Enterprise AI ETL Engine",
    layout="wide"
)

# ==========================================================
# HEADER STYLE
# ==========================================================
st.markdown("""
<style>
.header {
    background-color:#b31b1b;
    padding:20px;
    text-align:center;
}
.header h1{
    color:#ffcc00;
    margin:0;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="header">
<h1>Enterprise AI ETL Engine</h1>
</div>
""", unsafe_allow_html=True)

st.divider()

# ==========================================================
# HUGGINGFACE CLIENT
# ==========================================================
HF_TOKEN = st.secrets["HF_TOKEN"]

client = InferenceClient(
    model="mistralai/Mistral-7B-Instruct-v0.2",
    token=HF_TOKEN
)

# ==========================================================
# LLM → PANDAS CODE GENERATOR
# ==========================================================
def generate_pandas_code(columns, user_prompt):

    system_prompt = f"""
You are an expert Pandas engineer.

DataFrame name: df
Available columns: {columns}

Rules:
- Return ONLY executable pandas code.
- Do NOT explain anything.
- Do NOT use markdown.
- Do NOT import libraries.
- Modify df directly.
- Final output must remain in variable df.
"""

    full_prompt = system_prompt + "\nUser request: " + user_prompt

    response = client.text_generation(
        full_prompt,
        max_new_tokens=300,
        temperature=0.1
    )

    # Clean accidental markdown
    response = re.sub(r"```.*?```", "", response, flags=re.DOTALL)
    return response.strip()

# ==========================================================
# SAFE EXECUTION
# ==========================================================
def execute_code(df, code):

    allowed_globals = {"pd": pd}
    local_vars = {"df": df.copy()}

    exec(code, allowed_globals, local_vars)

    return local_vars["df"]

# ==========================================================
# UI
# ==========================================================
uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
prompt = st.text_area("Describe transformation")

if st.button("Run AI Transformation"):

    if not uploaded_file or not prompt:
        st.error("Upload file and enter transformation.")
    else:
        df = pd.read_csv(uploaded_file)

        st.subheader("Original Data")
        st.dataframe(df.head())

        with st.spinner("Generating Pandas transformation via AI..."):
            code = generate_pandas_code(df.columns.tolist(), prompt)

        st.subheader("Generated Pandas Code")
        st.code(code, language="python")

        try:
            updated_df = execute_code(df, code)

            st.subheader("Transformed Data")
            st.dataframe(updated_df)

            csv = BytesIO()
            updated_df.to_csv(csv, index=False)

            st.download_button(
                "Download Result",
                csv.getvalue(),
                file_name="transformed_output.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"Execution Error: {e}")
