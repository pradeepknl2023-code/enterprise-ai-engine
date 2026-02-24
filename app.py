import streamlit as st
from huggingface_hub import InferenceClient
import os
import re

# ------------------ Custom CSS (Full Wells Fargo Page Header + Professional Font) ------------------
st.markdown(
    """
    <style>
    /* Full-page header */
    .page-header {
        width: 100%;
        background-color: #E41B17; /* Wells Fargo Red */
        color: #FFD700;           /* Wells Fargo Yellow */
        padding: 25px 20px;
        font-family: 'Poppins', sans-serif;
        text-align: left;
        font-size: 2rem;
        font-weight: 700;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        border-radius: 0px;
        margin-bottom: 10px;
    }

    /* Subtitle / description */
    .page-subtitle {
        width: 100%;
        text-align: left;
        font-size: 1rem;   /* smaller, professional font */
        color: #333333;
        margin-bottom: 25px;
        font-weight: 500;
    }

    /* Buttons */
    div.stButton > button:first-child {
        background-color: #E41B17;
        color: #FFD700;
        border-radius: 12px;
        padding: 10px 25px;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    div.stButton > button:first-child:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.2);
    }

    /* Textarea input */
    textarea {
        border-radius: 12px !important;
        padding: 10px !important;
        font-size: 16px;
        border: 1px solid #E41B17 !important;
    }

    /* Epic cards */
    .epic {
        background: #FFF5F5; 
        padding: 15px;
        border-radius: 15px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        margin-bottom: 20px;
        border-left: 5px solid #E41B17;
    }

    /* Story cards */
    .story-card {
        background: #ffffff;
        border-radius: 15px;
        padding: 15px;
        margin-bottom: 15px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        border-left: 5px solid #E41B17;
    }

    /* Download button */
    .stDownloadButton>button {
        background-color: #E41B17;
        color: #FFD700;
        border-radius: 12px;
        padding: 8px 20px;
        font-weight: 600;
    }
    .stDownloadButton>button:hover {
        transform: translateY(-1px);
        box-shadow: 0 6px 15px rgba(0,0,0,0.2);
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ------------------ Full Width Header ------------------
st.markdown('<div class="page-header">AI‑PO‑Assistantce</div>', unsafe_allow_html=True)
st.markdown('<div class="page-subtitle">Convert Enterprise Business Requirements into Jira-ready Markdown</div>', unsafe_allow_html=True)

# ------------------ Hugging Face Setup ------------------
HF_TOKEN = os.getenv("HF_TOKEN")
if not HF_TOKEN:
    st.error("HF_TOKEN not found. Add it in Settings → Secrets on Streamlit Cloud.")
    st.stop()

client = InferenceClient(token=HF_TOKEN)

# ------------------ User Input ------------------
st.markdown("### Enter Business Requirement")
requirement_text = st.text_area("📄 Type Business Requirement", height=200)
uploaded_file = st.file_uploader("📂 Or upload a .txt file with the requirement", type=["txt"])

requirement_file = ""
if uploaded_file is not None:
    try:
        requirement_file = uploaded_file.read().decode("utf-8", errors="ignore")
        st.info("Uploaded file preview (first 500 characters):")
        st.text(requirement_file[:500])
    except Exception as e:
        st.error(f"Error reading file: {e}")

requirement = requirement_file if uploaded_file else requirement_text

# ------------------ Generate Breakdown ------------------
if st.button("🚀 Generate Breakdown"):

    if not requirement.strip():
        st.warning("Please enter or upload a requirement.")
    else:
        with st.spinner("Generating structured breakdown..."):

            prompt = f"""
You are a Senior Enterprise Business Analyst with expertise in Jira and Agile methodology.

Convert the following business requirement into structured Markdown suitable for Jira.

Guidelines:
- Create a clear Epic with Title and Description.
- Generate multiple User Stories if required.
- Each User Story should have:
    - Title
    - Role (As a ...)
    - Goal (I want ...)
    - Reason (So that ...)
- Include Acceptance Criteria for each story in bullet points.
- Include Subtasks for each story in bullet points.

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
                output = response.choices[0].message.content
            except Exception as e:
                st.error(f"Error calling Hugging Face API: {e}")
                st.stop()

            # ------------------ Display Epic ------------------
            epic_match = re.search(
                r"# Epic\s*- Title:(.*?)(?:- Description:)?(.*?)(?=\n# User Stories)",
                output,
                re.DOTALL
            )
            if epic_match:
                epic_title = epic_match.group(1).strip()
                epic_desc = epic_match.group(2).strip()
                st.markdown(f"""
<div class="epic">
<h3>🏷️ Epic</h3>
<p><b>Title:</b> {epic_title}</p>
<p><b>Description:</b> {epic_desc}</p>
</div>
""", unsafe_allow_html=True)

            # ------------------ Display User Stories ------------------
            user_stories = re.split(r"## Story \d+: Title", output)
            if len(user_stories) > 1:
                st.markdown("## 📋 User Stories")
                for i, story in enumerate(user_stories[1:], 1):
                    story_text = story.strip()\
                        .replace("**As a**", "**Role:**")\
                        .replace("**I want**", "**Goal:**")\
                        .replace("**So that**", "**Reason:**")
                    st.markdown(f"""
<div class="story-card">
<h4>Story {i}</h4>
<p>{story_text.replace(chr(10), '<br>')}</p>
</div>
""", unsafe_allow_html=True)
            else:
                st.markdown(output)

            # ------------------ Download Button ------------------
            st.download_button(
                label="💾 Download Jira Markdown",
                data=output,
                file_name="jira_breakdown.md",
                mime="text/markdown"
            )
