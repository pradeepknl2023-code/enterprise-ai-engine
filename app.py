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
.stButton button {
    background-color:#b31b1b;
    color:white;
    font-weight:bold;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="header">
<h1>Enterprise AI ETL Engine (Free AI Mode)</h1>
</div>
""", unsafe_allow_html=True)

st.divider()

# ==========================================================
# HUGGINGFACE CLIENT (STABLE FREE MODEL)
# ==========================================================

HF_TOKEN = st.secrets["HF_TOKEN"]

client = InferenceClient(
    model="HuggingFaceH4/zephyr-7b-beta",
    token=HF_TOKEN
)

# ==========================================================
# LLM → PANDAS CODE GENERATOR
# ==========================================================
def generate_pandas_code(columns, user_prompt):

    system_prompt = f"""
You are a senior Pandas data engineer.

DataFrame name: df
Available columns: {columns}

STRICT RULES:
- Output ONLY executable Python pandas code.
- Do NOT explain anything.
- Do NOT use markdown.
- Do NOT import anything.
- Do NOT redefine df.
- Modify df directly.
- Final output must remain stored in df.
"""

    response = client.chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        max_tokens=400,
        temperature=0.1
    )

    content = response.choices[0].message.content

    # Clean markdown if model adds it
    content = re.sub(r"```.*?```", "", content, flags=re.DOTALL)
    content = content.replace("```python", "").replace("```", "").strip()

    return content

# ==========================================================
# SAFE EXECUTION ENGINE
# ==========================================================
def execute_code(df, code):

    # Block dangerous patterns
    forbidden = ["import", "__", "os.", "sys.", "eval", "exec", "open(", "subprocess"]
    for word in forbidden:
        if word in code:
            raise Exception("Unsafe code detected.")

    allowed_globals = {"pd": pd}
    local_vars = {"df": df.copy()}

    exec(code, allowed_globals, local_vars)

    return local_vars["df"]

# ==========================================================
# UI
# ==========================================================

uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])
prompt = st.text_area(
    "Describe your transformation",
    placeholder="Example: remove records where Email starts with pradeep"
)

if st.button("Run AI Transformation"):

    if not uploaded_file or not prompt:
        st.error("Upload file and enter transformation.")
    else:
        df = pd.read_csv(uploaded_file)

        st.subheader("Original Data")
        st.dataframe(df.head())

        with st.spinner("AI generating Pandas transformation..."):
            try:
                code = generate_pandas_code(df.columns.tolist(), prompt)
            except Exception as e:
                st.error(f"Model Error: {e}")
                st.stop()

        st.subheader("Generated Pandas Code")
        st.code(code, language="python")

        try:
            updated_df = execute_code(df, code)

            st.subheader("Transformed Data")
            st.dataframe(updated_df)

            csv_buffer = BytesIO()
            updated_df.to_csv(csv_buffer, index=False)

            st.download_button(
                "Download Transformed CSV",
                csv_buffer.getvalue(),
                file_name="transformed_output.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"Execution Error: {e}")
