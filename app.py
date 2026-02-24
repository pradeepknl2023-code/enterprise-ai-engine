import streamlit as st
from huggingface_hub import InferenceClient
import os
import re

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="AI-PO-Assistant",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --------------------------------------------------
# WELLS FARGO ENTERPRISE THEME
# --------------------------------------------------
st.markdown("""
<style>

/* Remove default spacing */
.block-container {
    padding-top: 0rem !important;
    padding-left: 2rem;
    padding-right: 2rem;
}

/* Full Width Red Header */
.page-header {
    width: 100vw;
    margin-left: calc(-50vw + 50%);
    background: linear-gradient(90deg, #B5121B, #E41B17);
    color: #FFD700;
    padding: 28px 40px;
    font-size: 28px;
    font-weight: 700;
    letter-spacing: 0.5px;
    text-align: left;
    box-shadow: 0 6px 18px rgba(0,0,0,0.25);
}

/* Dark subtitle strip */
.page-subtitle {
    width: 100vw;
    margin-left: calc(-50vw + 50%);
    background-color: #111827;
    color: #F3F4F6;
    padding: 10px 40px;
    font-size: 14px;
    font-weight: 500;
    margin-bottom: 30px;
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

/* Epic Card */
.epic {
    background: #FFF7F7;
    padding: 20px;
    border-radius: 12px;
    margin-bottom: 25px;
    border-left: 6px solid #E41B17;
}

/* Story Card */
.story-card {
    background: #FFFFFF;
    border-radius: 12px;
    padding: 18px;
    margin-bottom: 18px;
    border-left: 6px solid #E41B17;
    box-shadow: 0 4px 10px rgba(0,0,0,0.05);
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
# HEADER
# --------------------------------------------------
st.markdown('<div class="page-header">AI-PO-Assistant</div>', unsafe_allow_html=True)
st.markdown('<div class="page-subtitle">Convert Enterprise Business Requirements into Jira-ready Markdown</div>', unsafe_allow_html=True)

# --------------------------------------------------
# HUGGINGFACE CONFIG
# --------------------------------------------------
HF_TOKEN = os.environ.get("HF_TOKEN")

if not HF_TOKEN:
    st.error("HF_TOKEN not found. Add it in Settings → Secrets.")
    st.stop()

client = InferenceClient(token=HF_TOKEN)

MODEL_NAME = "mistralai/Mistral-7B-Instruct-v0.2"  # Stable model

# --------------------------------------------------
# INPUT SECTION
# --------------------------------------------------
st.markdown("### Enter Business Requirement")

requirement_text = st.text_area("📄 Type Business Requirement", height=200)
uploaded_file = st.file_uploader("📂 Or upload a .txt file", type=["txt"])

requirement_file = ""
if uploaded_file is not None:
    requirement_file = uploaded_file.read().decode("utf-8", errors="ignore")
    st.info("File uploaded successfully.")

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

Format:
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
            except Exception as e:
                st.error(f"HuggingFace API Error: {e}")
                st.stop()

            # Display Output
            st.markdown("## 📋 Jira Structured Output")
            st.markdown(output)

            # Download
            st.download_button(
                "💾 Download Markdown",
                output,
                file_name="jira_breakdown.md",
                mime="text/markdown"
            )
