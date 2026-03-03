
"""
job_aggregator_ui.py  — v2.0
Senior Job Aggregator UI — Dynamic keyword search, Indian sources, senior-friendly UX.
Run:  streamlit run job_aggregator_ui.py
"""

import streamlit as st
import pandas as pd
import asyncio
import sys
import re
from job_aggregator import main as fetch_jobs
from job_link_resolver import get_direct_link

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Senior Engineer Jobs",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# GLOBAL STYLES — Senior-friendly (18–22px fonts, WCAG AA contrast)
# ─────────────────────────────────────────────
st.markdown("""
<style>
html, body, [class*="css"] {
    font-size: 19px !important;
    font-family: 'Segoe UI', Arial, sans-serif !important;
    background-color: #F7F9FC !important;
    color: #1A1A2E !important;
}
/* ── Top search bar strip ─────────────────────────────────────────────────── */
.top-bar {
    background-color: #0057B8;
    padding: 1rem 1.6rem;
    border-radius: 10px;
    margin-bottom: 1.4rem;
    display: flex;
    align-items: center;
    gap: 1rem;
}
/* ── Sidebar ──────────────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background-color: #1A1A2E !important;
    color: #FFFFFF !important;
    padding: 1.5rem 1rem !important;
}
section[data-testid="stSidebar"] * { color: #FFFFFF !important; font-size: 18px !important; }
/* ── Headings ─────────────────────────────────────────────────────────────── */
h1 { font-size: 2rem !important; font-weight: 700; color: #1A1A2E !important; }
h2 { font-size: 1.6rem !important; font-weight: 600; }
h3 { font-size: 1.3rem !important; font-weight: 600; }
/* ── Breadcrumb ───────────────────────────────────────────────────────────── */
.breadcrumb {
    font-size: 0.95rem;
    color: #4A5568;
    margin-bottom: 0.8rem;
    padding: 0.4rem 0;
    border-bottom: 1px solid #E2E8F0;
}
/* ── Job card ─────────────────────────────────────────────────────────────── */
.job-card {
    background-color: #FFFFFF;
    border: 2px solid #D0D8E8;
    border-left: 6px solid #0057B8;
    border-radius: 10px;
    padding: 1.4rem 1.6rem;
    margin-bottom: 1.2rem;
    box-shadow: 0 2px 6px rgba(0,0,0,0.06);
}
.job-card:hover { border-left-color: #003D82; box-shadow: 0 4px 14px rgba(0,87,184,0.12); }
/* ── Job title ────────────────────────────────────────────────────────────── */
.job-title {
    font-size: 1.3rem !important;
    font-weight: 700;
    color: #1A1A2E;
    margin-bottom: 0.3rem;
    line-height: 1.4;
}
/* ── Meta ─────────────────────────────────────────────────────────────────── */
.job-meta {
    font-size: 1.05rem !important;
    color: #374151;
    margin-bottom: 0.6rem;
    line-height: 1.7;
}
.job-meta strong { color: #1A1A2E; }
/* ── Salary badge ─────────────────────────────────────────────────────────── */
.salary-badge {
    display: inline-block;
    background-color: #D1FAE5;
    color: #065F46;
    border: 1px solid #6EE7B7;
    border-radius: 20px;
    padding: 3px 14px;
    font-size: 1rem !important;
    font-weight: 600;
    margin-bottom: 0.8rem;
}
.salary-na {
    display: inline-block;
    background-color: #F3F4F6;
    color: #6B7280;
    border: 1px solid #D1D5DB;
    border-radius: 20px;
    padding: 3px 14px;
    font-size: 1rem !important;
    margin-bottom: 0.8rem;
}
/* ── Region tag ───────────────────────────────────────────────────────────── */
.region-tag {
    display: inline-block;
    background-color: #EFF6FF;
    color: #1D4ED8;
    border: 1px solid #BFDBFE;
    border-radius: 20px;
    padding: 3px 12px;
    font-size: 0.95rem !important;
    font-weight: 600;
    margin-left: 8px;
}
/* ── Apply button — LARGE, high-contrast ──────────────────────────────────── */
.apply-btn {
    display: inline-block;
    background-color: #0057B8;
    color: #FFFFFF !important;
    font-size: 1.1rem !important;
    font-weight: 700;
    padding: 14px 32px;
    border-radius: 8px;
    text-decoration: none !important;
    border: 3px solid transparent;
    cursor: pointer;
    margin-top: 0.6rem;
    letter-spacing: 0.3px;
}
.apply-btn:hover { background-color: #003D82; border-color: #001F4D; }
.apply-btn:focus { outline: 4px solid #F59E0B; outline-offset: 3px; }
/* ── Stats bar ────────────────────────────────────────────────────────────── */
.stats-bar {
    background-color: #EFF6FF;
    border: 1px solid #BFDBFE;
    border-radius: 8px;
    padding: 0.8rem 1.2rem;
    margin-bottom: 1.4rem;
    font-size: 1.05rem !important;
    color: #1D4ED8;
    font-weight: 600;
}
/* ── No results ───────────────────────────────────────────────────────────── */
.no-results {
    text-align: center;
    padding: 3rem;
    color: #6B7280;
    font-size: 1.1rem;
    background: #FFFFFF;
    border: 2px dashed #D1D5DB;
    border-radius: 10px;
    margin-top: 1.5rem;
}
hr { border-color: #E2E8F0 !important; margin: 1rem 0; }
.stSlider label, .stCheckbox label, .stSelectbox label, .stTextInput label {
    font-size: 18px !important;
    font-weight: 600 !important;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# SESSION STATE — keyword persistence
# ─────────────────────────────────────────────
if "keyword" not in st.session_state:
    st.session_state["keyword"] = "Piping Engineer"
if "jobs_df" not in st.session_state:
    st.session_state["jobs_df"] = pd.DataFrame()
if "last_keyword" not in st.session_state:
    st.session_state["last_keyword"] = ""

# ─────────────────────────────────────────────
# PERSISTENT TOP BAR (breadcrumb + search)
# ─────────────────────────────────────────────
st.markdown('''
<div class="breadcrumb">
    🏠 Home &nbsp;›&nbsp; <strong>Job Search</strong>
    &nbsp;|&nbsp; <em>Enter a keyword below and click Search to refresh results</em>
</div>
''', unsafe_allow_html=True)

col_search, col_btn = st.columns([5, 1])
with col_search:
    keyword_input = st.text_input(
        "🔍 Search for a role (e.g. Piping Engineer, Pipe Stress Analysis, FEED Manager)",
        value=st.session_state["keyword"],
        placeholder="Type any job title or skill…",
        label_visibility="visible",
        key="keyword_input_box",
    )
with col_btn:
    st.markdown("<br>", unsafe_allow_html=True)
    search_clicked = st.button("🔎 Search", use_container_width=True)

st.markdown("---")

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
REGION_FLAGS = {
    "UK": "🇬🇧", "India": "🇮🇳", "Singapore": "🇸🇬",
    "Malaysia": "🇲🇾", "Gulf": "🌍", "UAE": "🌍",
    "Saudi Arabia": "🌍", "Qatar": "🌍",
}

SENIORITY_FILTER = re.compile(
    r"\b(lead|principal|hod|chief|senior|head\s*of\s*dept|20\+\s*years?)\b",
    re.IGNORECASE,
)

import re

def render_job_card(row: pd.Series) -> None:
    flag       = REGION_FLAGS.get(row["region"], "🌐")
    salary_str = str(row.get("salary", "N/A")).strip()
    has_salary = salary_str not in ("N/A", "", "0", "nan")
    salary_html = (
        f'<span class="salary-badge">💰 {salary_str}</span>'
        if has_salary else
        '<span class="salary-na">Salary not disclosed</span>'
    )
    final_url  = str(row.get("final_url", row.get("url", "#"))).strip()
    source_lbl = str(row.get("source", "Company Site"))

    st.markdown(f"""
    <div class="job-card">
        <div class="job-title">
            🔧 {row['title']}
            <span class="region-tag">{flag} {row['region']}</span>
        </div>
        <div class="job-meta">
            <strong>🏢 Company:</strong> {row['company']}&nbsp;&nbsp;|&nbsp;&nbsp;
            <strong>📍 Location:</strong> {row['location']}&nbsp;&nbsp;|&nbsp;&nbsp;
            <strong>🔎 Source:</strong> {source_lbl}&nbsp;&nbsp;|&nbsp;&nbsp;
            <strong>📅 Listed:</strong> {row.get('scraped_at', 'N/A')}
        </div>
        {salary_html}
        <br/>
        <a class="apply-btn"
           href="{final_url}"
           target="_blank"
           rel="noopener noreferrer"
           aria-label="Apply for {row['title']} at {row['company']} — opens in new tab">
            🔗 Apply Directly on Company Site
        </a>
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────
# DATA LOADING — triggered on first run or new keyword
# ─────────────────────────────────────────────
keyword_to_run = keyword_input.strip() or "Piping Engineer"
needs_refresh  = (
    search_clicked
    or st.session_state["jobs_df"].empty
    or st.session_state["last_keyword"] != keyword_to_run
)

if needs_refresh:
    st.session_state["keyword"]      = keyword_to_run
    st.session_state["last_keyword"] = keyword_to_run

    with st.spinner(f"🔄 Fetching senior **{keyword_to_run}** roles across all regions…"):
        try:
            if sys.platform == "win32":
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            raw_df = asyncio.run(fetch_jobs(keyword_to_run))
        except Exception as e:
            st.error(f"⚠️ Error fetching jobs: {e}")
            raw_df = pd.DataFrame()

    if not raw_df.empty:
        with st.spinner("🔗 Resolving direct employer links…"):
            raw_df["final_url"] = raw_df["url"].apply(
                lambda u: get_direct_link(u) if str(u).startswith("http") else u
            )
    st.session_state["jobs_df"] = raw_df

df = st.session_state["jobs_df"].copy()

# ─────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔧 Filter Jobs")
    st.markdown("---")

    st.markdown("### 🌍 Regions")
    all_regions = sorted(df["region"].unique().tolist()) if not df.empty else ["India","UK","Gulf","Singapore","Malaysia"]
    selected_regions = []
    for region in all_regions:
        flag = REGION_FLAGS.get(region, "🌐")
        if st.checkbox(f"{flag}  {region}", value=True, key=f"region_{region}"):
            selected_regions.append(region)

    st.markdown("---")
    st.markdown("### 🏢 Sources")
    all_sources = sorted(df["source"].unique().tolist()) if not df.empty else []
    selected_sources = []
    for src in all_sources:
        if st.checkbox(src, value=True, key=f"src_{src}"):
            selected_sources.append(src)

    st.markdown("---")
    st.markdown("### 📝 Filter by Title Keyword")
    title_search = st.text_input(
        "E.g. FPSO, Offshore, LNG",
        placeholder="Leave blank to show all",
        label_visibility="visible",
    )

    st.markdown("---")
    st.markdown("### 📅 Sort By")
    sort_option = st.selectbox(
        "Sort listings by",
        options=["Most Recent", "Company A–Z", "Salary (High to Low)"],
        index=0,
    )

    st.markdown("---")
    if st.button("🔄 Clear Cache & Refresh", use_container_width=True):
        st.session_state["jobs_df"]      = pd.DataFrame()
        st.session_state["last_keyword"] = ""
        st.rerun()

# ─────────────────────────────────────────────
# APPLY SENIORITY POST-FILTER
# ─────────────────────────────────────────────
if not df.empty:
    df = df[
        df["title"].str.contains(SENIORITY_FILTER, na=False) |
        df["company"].str.contains(SENIORITY_FILTER, na=False)
    ]

# ─────────────────────────────────────────────
# APPLY UI FILTERS
# ─────────────────────────────────────────────
if not df.empty:
    if selected_regions:
        df = df[df["region"].isin(selected_regions)]
    if selected_sources:
        df = df[df["source"].isin(selected_sources)]
    if title_search.strip():
        df = df[df["title"].str.contains(title_search.strip(), case=False, na=False)]

    if sort_option == "Company A–Z":
        df = df.sort_values("company")
    elif sort_option == "Salary (High to Low)":
        df = df.sort_values("salary", ascending=False)
    else:
        df = df.sort_values("scraped_at", ascending=False)

    df = df.reset_index(drop=True)

# ─────────────────────────────────────────────
# MAIN HEADER
# ─────────────────────────────────────────────
st.markdown(f"# 🔧 Senior **{st.session_state['keyword']}** — Job Listings")
st.markdown(
    "Showing **Lead · Senior · Principal · HOD · Chief** roles &nbsp;|&nbsp; "
    "20+ years seniority filter applied &nbsp;|&nbsp; "
    "Sources: Adzuna, Jooble, Naukri, iimjobs, TimesJobs, TCE, Technip, L&T, EIL, IOCL"
)

# ─────────────────────────────────────────────
# STATS BAR
# ─────────────────────────────────────────────
if not df.empty:
    total = len(df)
    region_counts = " · ".join(
        f"{REGION_FLAGS.get(r,'🌐')} {r}: {n}"
        for r, n in df["region"].value_counts().items()
    )
    st.markdown(f"""
    <div class="stats-bar">
        📋 <strong>{total} job{"" if total == 1 else "s"} found</strong>
        &nbsp;|&nbsp; {region_counts}
    </div>
    """, unsafe_allow_html=True)

    for _, row in df.iterrows():
        render_job_card(row)
else:
    if not st.session_state["jobs_df"].empty:
        st.markdown("""
        <div class="no-results">
            <h3>😔 No senior roles match your current filters.</h3>
            <p>Try selecting more regions, clearing the title search, or adjusting source filters.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("👆 Enter a keyword above and click **Search** to load jobs.")

# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#9CA3AF; font-size:0.95rem;'>"
    "Sources: Adzuna · Jooble · Naukri.com · iimjobs · TimesJobs · "
    "TCE · Technip Energies · L&T Hydrocarbon · EIL · IOCL · "
    "Links resolved via job_link_resolver"
    "</div>",
    unsafe_allow_html=True,
)
