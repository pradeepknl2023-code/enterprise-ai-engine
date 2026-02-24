st.markdown("""
<style>

/* Remove ALL default padding */
.block-container {
    padding: 0rem !important;
}

/* Remove Streamlit width restriction */
section.main > div {
    max-width: 100% !important;
    padding-left: 0rem !important;
    padding-right: 0rem !important;
}

/* FULL WIDTH HEADER */
.page-header {
    width: 100%;
    background: linear-gradient(90deg, #B5121B, #E41B17);
    color: #FFD700;
    padding: 30px 60px;
    font-size: 30px;
    font-weight: 700;
    letter-spacing: 0.5px;
}

/* FULL WIDTH SUBTITLE */
.page-subtitle {
    width: 100%;
    background-color: #0F172A;
    color: #F8FAFC;
    padding: 12px 60px;
    font-size: 14px;
    font-weight: 500;
    margin-bottom: 40px;
}

/* Restore padding only for content AFTER header */
.content-wrapper {
    padding-left: 2rem;
    padding-right: 2rem;
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

</style>
""", unsafe_allow_html=True)
