import streamlit as st
from huggingface_hub import InferenceClient
import os

# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(
    page_title="AI-PO-Assistantce",
    layout="wide"
)

# =====================================================
# CUSTOM CSS (Enterprise Header Style)
# =====================================================
st.markdown(
    """
    <style>
    .page-header {
        width: 100%;
        background-color: #E41B17;
        color: #FFD700;
        padding: 25px 20px;
        font-size: 28px;
        font-weight: 700;
        text-align: left;
    }
    .page-subtitle {
        font-size: 14px;
        margin-bottom: 25px;
        color: #333333;
    }
    div.stButton > button {
        background-color: #E41B17;
        color: #FFD700;
        border-radius: 8px;
        padding: 8px 18px;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown('<div class="page-header">AI-PO-Assistantce</div>', unsafe_allow_html=True)
st.markdown('<div class="page-subtitle">Convert Enterprise Business Requirements into Jira-ready Markdown & Perform AI-based ETL Transformations</div>', unsafe_allow_html=True)

# =====================================================
# HUGGING FACE SETUP
# =====================================================
HF_TOKEN = os.getenv("HF_TOKEN")

if not HF_TOKEN:
    st.error("HF_TOKEN not found. Add it in Streamlit Cloud → Settings → Secrets.")
    st.stop()

client = InferenceClient(token=HF_TOKEN)

# =====================================================
# INPUT SECTION
# =====================================================
st.markdown("### 📄 Business Requirement / Transformation Description")
requirement_text = st.text_area("Enter Business Requirement", height=200)

uploaded_file = st.file_uploader("📂 Upload Input File (.txt)", type=["txt"])

file_content = ""
if uploaded_file is not None:
    file_content = uploaded_file.read().decode("utf-8", errors="ignore")
    st.info("Uploaded file preview (first 500 chars)")
    st.text(file_content[:500])

requirement = requirement_text.strip()

# =====================================================
# BUTTONS
# =====================================================
col1, col2 = st.columns(2)

# =====================================================
# 🚀 JIRA BREAKDOWN BUTTON
# =====================================================
with col1:
    if st.button("🚀 Generate Jira Breakdown"):

        if not requirement:
            st.warning("Please enter Business Requirement.")
        else:
            with st.spinner("Generating Jira structured output..."):

                prompt = f"""
You are a Senior Enterprise Business Analyst.

Convert the following business requirement into Jira-ready Markdown.

Include:
- Epic (Title + Description)
- Multiple User Stories
- Each Story must include:
    - Title
    - Role
    - Goal
    - Reason
    - Acceptance Criteria
    - Subtasks

Business Requirement:
{requirement}
"""

                try:
                    response = client.chat.completions.create(
                        model="Qwen/Qwen2.5-7B-Instruct",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=1500,
                        temperature=0.3
                    )

                    jira_output = response.choices[0].message.content

                    st.markdown("### 📋 Jira Markdown Output")
                    st.text_area("Output", jira_output, height=400)

                    st.download_button(
                        label="💾 Download Jira Markdown",
                        data=jira_output,
                        file_name="jira_output.md",
                        mime="text/markdown"
                    )

                except Exception as e:
                    st.error(f"API Error: {e}")

# =====================================================
# 🔄 ETL TRANSFORMATION BUTTON
# =====================================================
with col2:
    if st.button("🔄 Generate ETL Transformation"):

        if not requirement:
            st.warning("Please provide Transformation Description.")
        elif not file_content:
            st.warning("Please upload input file for ETL.")
        else:
            with st.spinner("Performing AI ETL transformation..."):

                etl_prompt = f"""
You are a Senior Data Engineer.

Perform ETL transformation based on the business logic provided.

Business Transformation Description:
{requirement}

Input Data:
{file_content}

Instructions:
1. Extract required fields.
2. Apply transformation rules.
3. Provide final structured clean output in CSV format.
4. Output only transformed data.
"""

                try:
                    response = client.chat.completions.create(
                        model="Qwen/Qwen2.5-7B-Instruct",
                        messages=[{"role": "user", "content": etl_prompt}],
                        max_tokens=2000,
                        temperature=0.2
                    )

                    etl_output = response.choices[0].message.content

                    st.markdown("### 📊 ETL Transformed Output")
                    st.text_area("Transformed Data", etl_output, height=400)

                    st.download_button(
                        label="💾 Download ETL Output",
                        data=etl_output,
                        file_name="etl_output.csv",
                        mime="text/csv"
                    )

                except Exception as e:
                    st.error(f"API Error: {e}")
