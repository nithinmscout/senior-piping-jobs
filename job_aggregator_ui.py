"""
job_aggregator_ui.py
─────────────────────────────────────────────────────────────────────────────
Streamlit UI — Senior Engineer Job Aggregator
─────────────────────────────────────────────────────────────────────────────
"""

import streamlit as st
import pandas as pd
import asyncio
import re

from job_aggregator import main as fetch_jobs
from job_link_resolver import get_direct_link

st.set_page_config(
    page_title="Senior Engineer Job Aggregator",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
html, body, [class*="css"] {
    font-size: 18px !important;
    font-family: 'Segoe UI', Arial, sans-serif !important;
    background-color: #F7F9FC !important;
    color: #1A1A2E !important;
}
section[data-testid="stSidebar"] {
    background-color: #1A1A2E !important;
    color: #FFFFFF !important;
    padding: 1.5rem 1rem !important;
}
section[data-testid="stSidebar"] * { color: #FFFFFF !important; font-size: 18px !important; }
section[data-testid="stSidebar"] .stCheckbox label { font-size: 18px !important; font-weight: 500; }
h1 { font-size: 2rem !important; font-weight: 700; color: #1A1A2E !important; }
h2 { font-size: 1.6rem !important; font-weight: 600; color: #1A1A2E !important; }
h3 { font-size: 1.3rem !important; font-weight: 600; color: #1A1A2E !important; }

.job-card {
    background-color: #FFFFFF;
    border: 2px solid #D0D8E8;
    border-left: 6px solid #0057B8;
    border-radius: 10px;
    padding: 1.4rem 1.6rem;
    margin-bottom: 1.2rem;
    box-shadow: 0 2px 6px rgba(0,0,0,0.06);
}
.job-card.priority-card {
    border-left: 6px solid #D97706;
    background-color: #FFFBEB;
}
.job-card:hover { border-left-color: #003D82; box-shadow: 0 4px 14px rgba(0,87,184,0.12); }
.job-title { font-size: 1.25rem !important; font-weight: 700; color: #1A1A2E; margin-bottom: 0.3rem; }
.job-meta  { font-size: 1rem !important; color: #4A5568; margin-bottom: 0.6rem; line-height: 1.6; }
.job-meta strong { color: #1A1A2E; }
.salary-badge {
    display: inline-block; background-color: #E8F4E8; color: #1B5E20;
    border: 1px solid #A5D6A7; border-radius: 20px; padding: 3px 14px;
    font-size: 0.95rem !important; font-weight: 600; margin-bottom: 0.8rem;
}
.salary-na {
    display: inline-block; background-color: #F3F4F6; color: #6B7280;
    border: 1px solid #D1D5DB; border-radius: 20px; padding: 3px 14px;
    font-size: 0.95rem !important; margin-bottom: 0.8rem;
}
.region-tag {
    display: inline-block; background-color: #EFF6FF; color: #1D4ED8;
    border: 1px solid #BFDBFE; border-radius: 20px; padding: 3px 12px;
    font-size: 0.9rem !important; font-weight: 600; margin-left: 8px;
}
.source-tag {
    display: inline-block; background-color: #FEF3C7; color: #92400E;
    border: 1px solid #FCD34D; border-radius: 20px; padding: 3px 10px;
    font-size: 0.85rem !important; font-weight: 600; margin-left: 6px;
}
.priority-tag {
    display: inline-block; background-color: #FEF3C7; color: #92400E;
    border: 1px solid #F59E0B; border-radius: 20px; padding: 3px 12px;
    font-size: 0.85rem !important; font-weight: 700; margin-left: 6px;
}
.apply-btn {
    display: inline-block; background-color: #0057B8; color: #FFFFFF !important;
    font-size: 1.05rem !important; font-weight: 700; padding: 12px 28px;
    border-radius: 8px; text-decoration: none !important; border: 3px solid transparent;
    cursor: pointer; margin-top: 0.6rem; letter-spacing: 0.3px;
}
.apply-btn:hover { background-color: #003D82; border-color: #001F4D; color: #FFFFFF !important; }
.apply-btn:focus { outline: 4px solid #F59E0B; outline-offset: 3px; }
.no-results {
    text-align: center; padding: 3rem; color: #6B7280; font-size: 1.1rem;
    background: #FFFFFF; border: 2px dashed #D1D5DB; border-radius: 10px; margin-top: 1.5rem;
}
.stats-bar {
    background-color: #EFF6FF; border: 1px solid #BFDBFE; border-radius: 8px;
    padding: 0.8rem 1.2rem; margin-bottom: 1.4rem;
    font-size: 1rem !important; color: #1D4ED8; font-weight: 600;
}
.exclude-bar {
    background-color: #FFF7ED; border: 1px solid #FED7AA; border-radius: 8px;
    padding: 0.8rem 1.2rem; margin-bottom: 1rem;
}
hr { border-color: #E2E8F0 !important; margin: 1rem 0; }
.stSlider label, .stCheckbox label, .stSelectbox label { font-size: 18px !important; font-weight: 600 !important; }
input[type="text"],
div[data-testid="stTextInput"] input {
    color: #1A1A2E !important; background-color: #FFFFFF !important;
    font-size: 18px !important; border: 2px solid #0057B8 !important;
    border-radius: 6px !important; padding: 8px 12px !important;
}
input[type="text"]::placeholder { color: #9CA3AF !important; opacity: 1 !important; }

/* ── Exclude multiselect — visible tags & input ───────── */
div[data-testid="stMultiSelect"] span[data-baseweb="tag"] {
    background-color: #C2410C !important;
    color: #FFFFFF !important;
    font-size: 1rem !important;
    font-weight: 600 !important;
    border-radius: 20px !important;
    padding: 4px 12px !important;
}
div[data-testid="stMultiSelect"] input {
    color: #1A1A2E !important;
    background-color: #FFFFFF !important;
    font-size: 17px !important;
    border: 2px solid #C2410C !important;
    border-radius: 6px !important;
}
div[data-testid="stMultiSelect"] > div {
    background-color: #FFF7ED !important;
    border: 2px solid #C2410C !important;
    border-radius: 8px !important;
}
div[data-testid="stMultiSelect"] label {
    color: #C2410C !important;
    font-weight: 700 !important;
    font-size: 1rem !important;
}
div[data-testid="stTextInput"]:has(input[placeholder*="offshore"]) input {
    border: 2px solid #C2410C !important;
    background-color: #FFF7ED !important;
    color: #1A1A2E !important;
}

</style>
""", unsafe_allow_html=True)

REGION_FLAGS = {
    "UK": "🇬🇧", "India": "🇮🇳", "Singapore": "🇸🇬",
    "Malaysia": "🇲🇾", "Gulf": "🌍", "UAE": "🇦🇪",
    "Saudi Arabia": "🇸🇦", "Qatar": "🇶🇦", "Global": "🌐",
}

EXCLUDE_PRESETS = [
    "HVAC", "Electrical", "Civil", "Structural", "Instrumentation",
    "Graduate", "Trainee", "Junior", "Drilling", "Well", "Telecom",
    "Software", "IT", "Sales", "Plumbing", "Contract Only",
]

# ─────────────────────────────────────────────
# TOP BAR
# ─────────────────────────────────────────────
st.markdown("# 🔧 Senior Engineer — Job Aggregator")
st.markdown(
    "**UK · India · Gulf · Singapore · Malaysia** | "
    "Adzuna · Jooble · Indeed · Naukri · iimjobs · Foundit · TimesJobs · "
    "Rigzone · EnergyJobSearch · NaukriGulf · GulfTalent · Bayt · "
    "Airswift · NES Fircroft · Orion · TRS · CV-Library · "
    "TCE · Technip · L&T · EIL · IOCL · Worley · KBR · Wood · Jacobs · AECOM + more"
)
st.markdown("---")

# ── Row 1: search box + button ───────────────────────────────────────────────
col_search, col_btn = st.columns([5, 1])
with col_search:
    search_query = st.text_input(
        label="🔍 Enter Job Title / Keyword",
        value=st.session_state.get("last_query", "Senior Piping Engineer"),
        placeholder="e.g.  Pipe Stress Analysis,  FEED Manager,  Principal Piping",
        help="Type any role title. The app will search all sources immediately.",
        label_visibility="visible",
    )
with col_btn:
    st.markdown("<br/>", unsafe_allow_html=True)
    run_search = st.button("🔄 Search", use_container_width=True)

# ── Row 2: exclude keywords (inline, below search bar) ──────────────────────
col_excl, col_custom = st.columns([3, 2])
with col_excl:
    user_exclusions = st.multiselect(
        label="🚫 Exclude keywords from results",
        options=EXCLUDE_PRESETS,
        default=[],
        help="Jobs whose title contains any selected word will be hidden instantly.",
        placeholder="Select words to exclude…",
    )
with col_custom:
    custom_exclude = st.text_input(
        label="➕ Add custom exclude words (comma-separated)",
        placeholder="e.g.  offshore, temporary, contract",
        label_visibility="visible",
    )

all_exclusions = user_exclusions[:]
if custom_exclude.strip():
    all_exclusions += [w.strip() for w in custom_exclude.split(",") if w.strip()]

if all_exclusions:
    st.markdown(
        f'<div class="exclude-bar">🚫 <strong>Active exclusions:</strong> '
        + " · ".join(f"<code>{w}</code>" for w in all_exclusions)
        + '</div>',
        unsafe_allow_html=True,
    )

st.markdown("---")

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
if "last_query"  not in st.session_state: st.session_state["last_query"]  = "Senior Piping Engineer"
if "job_df"      not in st.session_state: st.session_state["job_df"]      = pd.DataFrame()
if "fetch_done"  not in st.session_state: st.session_state["fetch_done"]  = False

query_changed = search_query.strip() != st.session_state["last_query"]
trigger_fetch = run_search or not st.session_state["fetch_done"] or query_changed

if trigger_fetch and search_query.strip():
    st.session_state["last_query"] = search_query.strip()
    with st.spinner(f"Searching all sources for **{search_query.strip()}** …"):
        try:
            df_raw = asyncio.run(fetch_jobs(query=search_query.strip()))
            if not df_raw.empty and "url" in df_raw.columns:
                df_raw["final_url"] = df_raw["url"].apply(get_direct_link)
            else:
                df_raw["final_url"] = df_raw.get("url", "N/A")
            df_raw["salary_num"] = (
                df_raw["salary"]
                .str.extract(r"(\d[\d,]*)", expand=False)
                .str.replace(",", "", regex=False)
                .fillna(0).astype(float).astype(int)
            )
            if "rank"     not in df_raw.columns: df_raw["rank"]     = 2
            if "priority" not in df_raw.columns: df_raw["priority"] = False
            st.session_state["job_df"]    = df_raw
            st.session_state["fetch_done"] = True
        except Exception as e:
            st.error(f"⚠️ Could not fetch jobs: {e}")
            st.session_state["fetch_done"] = False

df = st.session_state["job_df"]

# ─────────────────────────────────────────────
# SIDEBAR FILTERS  (regions, sources, priority, salary, sort)
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔧 Filter Jobs")
    st.markdown("---")

    st.markdown("### 🌍 Regions")
    all_regions = sorted(df["region"].unique().tolist()) if not df.empty else [
        "UK", "India", "Singapore", "Malaysia", "UAE", "Saudi Arabia", "Qatar", "Global"
    ]
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
    st.markdown("### ⭐ Priority Companies Only")
    priority_only = st.checkbox("Show only priority EPC / OG companies", value=False)

    st.markdown("---")
    st.markdown("### 💰 Minimum Salary (₹)")
    st.caption("ℹ️ Jobs with undisclosed salary are always shown.")
    min_salary = st.slider(
        "Minimum salary (₹)", min_value=0, max_value=10_000_000,
        value=0, step=100_000, label_visibility="collapsed",
    )
    if min_salary >= 10_000_000:
        sal_label = f"₹{min_salary/10_000_000:.1f} Cr"
    elif min_salary >= 100_000:
        sal_label = f"₹{min_salary/100_000:.1f} L"
    else:
        sal_label = f"₹{min_salary:,}"
    st.markdown(f"**Set minimum:** `{sal_label}`")

    st.markdown("---")
    st.markdown("### 📅 Sort By")
    sort_option = st.selectbox(
        "Sort listings by",
        options=["Relevance (Piping First)", "Most Recent", "Company A–Z", "Salary (High to Low)"],
        index=0,
    )

# ─────────────────────────────────────────────
# APPLY FILTERS
# ─────────────────────────────────────────────
if df.empty:
    st.markdown("""
    <div class="no-results">
        <h3>🔍 Enter a keyword above and press Search to begin.</h3>
        <p>Default search: <strong>Senior Piping Engineer</strong> across all regions.</p>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

filtered = df.copy()

if selected_regions:
    filtered = filtered[filtered["region"].isin(selected_regions)]
if selected_sources:
    filtered = filtered[filtered["source"].isin(selected_sources)]
if priority_only:
    filtered = filtered[filtered["priority"] == True]

if all_exclusions:
    pattern   = "|".join(re.escape(w) for w in all_exclusions)
    excl_mask = filtered["title"].str.contains(pattern, case=False, na=False)
    filtered  = filtered[~excl_mask]

salary_mask = (filtered["salary_num"] == 0) | (filtered["salary_num"] >= min_salary)
filtered = filtered[salary_mask]

if sort_option == "Relevance (Piping First)":
    filtered = filtered.assign(priority_sort=~filtered["priority"]).sort_values(
        ["priority_sort", "rank"], ascending=[True, True]
    ).drop(columns=["priority_sort"])
elif sort_option == "Company A–Z":
    filtered = filtered.sort_values("company")
elif sort_option == "Salary (High to Low)":
    filtered = filtered.sort_values("salary_num", ascending=False)
else:
    filtered = filtered.sort_values("scraped_at", ascending=False)

filtered = filtered.reset_index(drop=True)

# ─────────────────────────────────────────────
# STATS BAR
# ─────────────────────────────────────────────
total       = len(filtered)
with_salary = (filtered["salary_num"] > 0).sum()
priority_ct = int(filtered["priority"].sum()) if "priority" in filtered.columns else 0
region_counts = " · ".join(
    f"{REGION_FLAGS.get(r, '🌐')} {r}: {n}"
    for r, n in filtered["region"].value_counts().items()
)
st.markdown(f"""
<div class="stats-bar">
    📋 <strong>{total} job{'s' if total != 1 else ''} found</strong>
    &nbsp;|&nbsp; ⭐ {priority_ct} priority companies
    &nbsp;|&nbsp; 💰 {with_salary} with salary
    &nbsp;|&nbsp; {region_counts}
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# JOB CARDS
# ─────────────────────────────────────────────
def render_job_card(row: pd.Series) -> None:
    flag       = REGION_FLAGS.get(row["region"], "🌐")
    salary_str = str(row.get("salary", "N/A")).strip()
    has_salary = salary_str not in ("N/A", "", "0", "nan")
    salary_html = (
        f'<span class="salary-badge">💰 {salary_str}</span>'
        if has_salary else '<span class="salary-na">Salary not disclosed</span>'
    )
    final_url    = str(row.get("final_url", row.get("url", "#"))).strip()
    source_tag   = f'<span class="source-tag">📡 {row["source"]}</span>'
    priority_tag = '<span class="priority-tag">⭐ Priority Company</span>' if row.get("priority") else ""
    card_class   = "job-card priority-card" if row.get("priority") else "job-card"

    st.markdown(f"""
    <div class="{card_class}">
        <div class="job-title">
            🔧 {row['title']}
            <span class="region-tag">{flag} {row['region']}</span>
            {source_tag}{priority_tag}
        </div>
        <div class="job-meta">
            <strong>🏢 Company:</strong> {row['company']}&nbsp;&nbsp;|&nbsp;&nbsp;
            <strong>📍 Location:</strong> {row['location']}
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


if filtered.empty:
    st.markdown("""
    <div class="no-results">
        <h3>😔 No jobs match your current filters.</h3>
        <p>Try adjusting regions, sources, or clearing the exclusion keywords.</p>
    </div>
    """, unsafe_allow_html=True)
else:
    for _, row in filtered.iterrows():
        render_job_card(row)

st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#9CA3AF; font-size:0.9rem;'>"
    "Sources: Adzuna · Jooble · Indeed · Naukri · iimjobs · Foundit · TimesJobs · Rigzone · "
    "EnergyJobSearch · NaukriGulf · GulfTalent · Bayt · Airswift · NES Fircroft · Orion · "
    "TRS · CV-Library · EngineeringJobs · TCE · Technip · L&T · EIL · IOCL · "
    "Worley · KBR · Wood · Jacobs · AECOM · Bechtel · Penspen · AtkinsRéalis · GAIL · ONGC · "
    "Links resolved via job_link_resolver"
    "</div>",
    unsafe_allow_html=True,
)
