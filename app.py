import streamlit as st
from huggingface_hub import InferenceClient
import os

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Requirements Intelligence Platform",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --------------------------------------------------
# HUGGINGFACE CONFIG
# --------------------------------------------------
HF_TOKEN = os.getenv("HF_TOKEN")  # Set in Streamlit secrets or HF Spaces
MODEL_NAME = "meta-llama/Meta-Llama-3-8B-Instruct"

client = InferenceClient(
    model=MODEL_NAME,
    token=HF_TOKEN
)

# --------------------------------------------------
# STYLING
# --------------------------------------------------
st.markdown("""
<style>
.block-container {
    padding-top: 0rem !important;
    padding-left: 2rem;
    padding-right: 2rem;
}

.main-header {
    background: linear-gradient(90deg, #b5121b, #d71e28);
    padding: 25px 10px;
    text-align: center;
    color: #ffd700;
    font-size: 26px;
    font-weight: 700;
    width: 100vw;
    margin-left: calc(-50vw + 50%);
}

.sub-header {
    text-align: center;
    font-size: 14px;
    color: #f0f0f0;
    background-color: #0e1a2b;
    padding: 8px;
    width: 100vw;
    margin-left: calc(-50vw + 50%);
}

.section-title {
    font-size: 18px;
    font-weight: 600;
    margin-top: 25px;
}

body, p, div {
    font-size: 14px !important;
}
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# HEADER
# --------------------------------------------------
st.markdown('<div class="main-header">Requirements Intelligence Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Enterprise AI Requirements → Jira Story Engine</div>', unsafe_allow_html=True)

# --------------------------------------------------
# INPUT
# --------------------------------------------------
st.markdown('<div class="section-title">Enter Business Requirement</div>', unsafe_allow_html=True)

business_input = st.text_area(
    "",
    height=200,
    placeholder="Example: Build a secure loan transfer system with approval workflow and dashboard tracking..."
)

# --------------------------------------------------
# PROMPT TEMPLATE
# --------------------------------------------------
def build_prompt(requirement):
    return f"""
You are a senior Agile Business Analyst.

Convert the following enterprise business requirement into Jira-ready Markdown format.

Format strictly as:
- Epic
- Multiple User Stories
- Each story must include:
    - As a / I want / So that
    - Acceptance Criteria (bullet list)
    - Subtasks (bullet list)

Business Requirement:
{requirement}
"""

# --------------------------------------------------
# GENERATE BUTTON
# --------------------------------------------------
if st.button("Generate AI Jira Markdown"):

    if not business_input.strip():
        st.warning("Please enter business requirement.")
    else:
        with st.spinner("Generating AI-powered Jira stories..."):

            response = client.text_generation(
                build_prompt(business_input),
                max_new_tokens=1200,
                temperature=0.4
            )

        st.markdown('<div class="section-title">Jira-Ready Markdown Output</div>', unsafe_allow_html=True)
        st.code(response, language="markdown")

        st.download_button(
            "Download Markdown",
            response,
            file_name="jira_requirements.md",
            mime="text/markdown"
        )
