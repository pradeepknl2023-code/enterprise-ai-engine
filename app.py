import streamlit as st
from huggingface_hub import InferenceClient
import os

# --------------------------------------------------
# PAGE CONFIG (MUST BE FIRST)
# --------------------------------------------------
st.set_page_config(
    page_title="AI-PO-Assistant",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --------------------------------------------------
# FORCE TRUE FULL-WIDTH HEADER
# --------------------------------------------------
st.markdown("""
<style>

/* Remove Streamlit default padding */
.block-container {
    padding-top: 0rem !important;
    padding-left: 0rem !important;
    padding-right: 0rem !important;
}

/* Remove max width restriction */
section.main > div {
    max-width: 100% !important;
    padding-left: 0rem !important;
    padding-right: 0rem !important;
}

/* FULL WIDTH HEADER */
.page-header {
    background: linear-gradient(90deg, #B5121B, #E41B17);
    color: #FFD700;
    padding: 30px 60px;
    font-size: 30px;
    font-weight: 700;
    letter-spacing: 0.5px;
}

/* FULL WIDTH SUBTITLE */
.page-subtitle {
    background-color: #0F172A;
    color: #F8FAFC;
    padding: 12px 60px;
    font-size: 14px;
    font-weight: 500;
    margin-bottom: 40px;
}

/* Content wrapper restores padding */
.content {
    padding-left: 3rem;
    padding-right: 3rem;
}

/* Buttons */
div.stButton > button:first-child {
    background-color: #E41B17;
    color: #FFD700;
    border-radius: 10px;
    padding: 10px 28px;
    font-weight: 600;
    border: none;
}

div.stButton > button:first-child:hover {
    background-color: #B5121B;
}

/* Textarea */
textarea {
    border-radius: 10px !important;
    padding: 12px !important;
    font-size: 15px !important;
    border: 2px solid #E41B17 !important;
}

.stDownloadButton>button {
    background-color: #E41B17;
    color: #FFD700;
    border-radius: 10px;
    font-weight: 600;
}

</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# HEADER (EDGE TO EDGE)
# --------------------------------------------------
st.markdown('<div class="page-header">AI-PO-Assistant</div>', unsafe_allow_html=True)
st.markdown('<div class="page-subtitle">Convert Enterprise Business Requirements into Jira-ready Markdown</div>', unsafe_allow_html=True)

# --------------------------------------------------
# START CONTENT AREA
# --------------------------------------------------
st.markdown('<div class="content">', unsafe_allow_html=True)

# --------------------------------------------------
# HUGGINGFACE CONFIG
# --------------------------------------------------
HF_TOKEN = os.environ.get("HF_TOKEN")

if not HF_TOKEN:
    st.error("HF_TOKEN not found. Add it in Settings → Secrets.")
    st.stop()

client = InferenceClient(token=HF_TOKEN)
MODEL_NAME = "mistralai/Mistral-7B-Instruct-v0.2"

# --------------------------------------------------
# INPUT
# --------------------------------------------------
st.markdown("### Enter Business Requirement")

requirement_text = st.text_area(
    "📄 Type Business Requirement",
    height=200
)

uploaded_file = st.file_uploader(
    "📂 Or upload a .txt file",
    type=["txt"]
)

requirement_file = ""
if uploaded_file:
    requirement_file = uploaded_file.read().decode("utf-8", errors="ignore")
    st.success("File uploaded successfully.")

requirement = requirement_file if uploaded_file else requirement_text

# --------------------------------------------------
# GENERATE
# --------------------------------------------------
if st.button("🚀 Generate Breakdown"):

    if not requirement.strip():
        st.warning("Please enter or upload a requirement.")
    else:
        with st.spinner("Generating structured Jira breakdown..."):

            prompt = f"""
You are a Senior Enterprise Business Analyst.

Convert the following business requirement into structured Markdown.

Format strictly as:

# Epic
- Title:
- Description:

# User Stories

## Story 1
- Title:
- As a:
- I want:
- So that:
- Acceptance Criteria:
- Subtasks:

Business Requirement:
{requirement}
"""

            try:
                output = client.text_generation(
                    prompt,
                    model=MODEL_NAME,
                    max_new_tokens=1200,
                    temperature=0.3
                )

                st.markdown("## 📋 Jira Structured Output")
                st.markdown(output)

                st.download_button(
                    "💾 Download Markdown",
                    output,
                    file_name="jira_breakdown.md",
                    mime="text/markdown"
                )

            except Exception as e:
                st.error(f"HuggingFace API Error: {e}")

# --------------------------------------------------
# CLOSE CONTENT DIV
# --------------------------------------------------
st.markdown('</div>', unsafe_allow_html=True)
