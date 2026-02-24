import streamlit as st
from huggingface_hub import InferenceClient
import os
import re

# ------------------ Page Config ------------------
st.set_page_config(
    page_title="Requirements Intelligence Platform",
    layout="wide"
)

# ------------------ Enterprise Dark Theme ------------------
st.markdown("""
<style>

/* Remove default Streamlit padding */
.block-container {
    padding-top: 0rem !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
}

/* Full page background */
.stApp {
    background-color: #0f172a;  /* Dark background */
    color: white;
    font-family: Arial, sans-serif;
}

/* Full width header */
.enterprise-header {
    width: 100%;
    padding: 25px;
    font-size: 28px;
    font-weight: bold;
    background-color: #b91c1c;  /* Wells Fargo red */
    color: #fbbf24;             /* Yellow text */
    letter-spacing: 1px;
    text-align: center;
    text-shadow: 1px 1px 2px rgba(0,0,0,0.5);
}

/* Subtitle */
.enterprise-subtitle {
    text-align: center;
    font-size: 16px;
    padding: 10px 0 25px 0;
    color: #e2e8f0;
}

/* Section titles */
h3 {
    color: #fbbf24;
}

/* Buttons */
div.stButton > button:first-child {
    background-color: #b91c1c;
    color: #fbbf24;
    border-radius: 8px;
    padding: 10px 25px;
    font-weight: bold;
    border: none;
}
div.stButton > button:first-child:hover {
    background-color: #991b1b;
}

/* Download Button */
.stDownloadButton > button {
    background-color: #b91c1c;
    color: #fbbf24;
    border-radius: 8px;
    padding: 8px 20px;
    font-weight: bold;
    border: none;
}
.stDownloadButton > button:hover {
    background-color: #991b1b;
}

/* Textarea */
textarea {
    background-color: #1e293b !important;
    color: white !important;
    border: 1px solid #b91c1c !important;
    border-radius: 8px !important;
}

/* Cards */
.epic, .story-card {
    background: #1e293b;
    padding: 15px;
    border-radius: 10px;
    margin-bottom: 15px;
    border-left: 4px solid #b91c1c;
}

/* Footer */
.enterprise-footer {
    text-align: center;
    padding: 15px;
    font-size: 14px;
    background: #1e293b;
    color: #cbd5e1;
    margin-top: 40px;
}

</style>
""", unsafe_allow_html=True)

# ------------------ Header ------------------
st.markdown(
    '<div class="enterprise-header">Requirements Intelligence Platform</div>',
    unsafe_allow_html=True
)

st.markdown(
    '<div class="enterprise-subtitle">Enterprise AI Requirements & Story Engine</div>',
    unsafe_allow_html=True
)

# ------------------ Hugging Face Setup ------------------
HF_TOKEN = os.getenv("HF_TOKEN")

if not HF_TOKEN:
    st.error("HF_TOKEN not found. Add it in Settings → Secrets.")
    st.stop()

client = InferenceClient(token=HF_TOKEN)

# ------------------ User Input ------------------
st.markdown("### Enter Business Requirement")

requirement_text = st.text_area(
    "Type Business Requirement",
    height=200
)

uploaded_file = st.file_uploader(
    "Or upload a .txt file",
    type=["txt"]
)

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
if st.button("Generate Breakdown"):

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
- Include Acceptance Criteria in bullet points.
- Include Subtasks in bullet points.

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
                <h3>Epic</h3>
                <p><b>Title:</b> {epic_title}</p>
                <p><b>Description:</b> {epic_desc}</p>
                </div>
                """, unsafe_allow_html=True)

            # ------------------ Display User Stories ------------------
            user_stories = re.split(r"## Story \d+: Title", output)

            if len(user_stories) > 1:
                st.markdown("### User Stories")

                for i, story in enumerate(user_stories[1:], 1):
                    story_text = story.strip()

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
                label="Download Jira Markdown",
                data=output,
                file_name="jira_breakdown.md",
                mime="text/markdown"
            )

# ------------------ Footer ------------------
st.markdown(
    '<div class="enterprise-footer">© 2026 Requirements Intelligence Platform | Enterprise AI Suite</div>',
    unsafe_allow_html=True
)
