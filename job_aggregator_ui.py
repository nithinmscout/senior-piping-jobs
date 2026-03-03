"""
job_aggregator_ui.py
─────────────────────────────────────────────────────────────────────────────
Streamlit UI for the Senior Piping Engineer Job Aggregator.
Senior-friendly UX: 18px+ fonts, high-contrast, row-based job cards.
─────────────────────────────────────────────────────────────────────────────
Run:
    streamlit run job_aggregator_ui.py
"""

import streamlit as st
import pandas as pd
import asyncio
import sys
import os

# ── Local module imports (place in same directory) ───────────────────────────
# from job_aggregator import main as fetch_jobs        # async aggregator
# from job_link_resolver import get_direct_link        # URL resolver

# ─────────────────────────────────────────────
# PAGE CONFIG — must be first Streamlit call
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Senior Piping Engineer Jobs",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# GLOBAL STYLES
# Senior-friendly: min 18px fonts, WCAG AA contrast ratios,
# clear focus rings, no animations, no hidden menus.
# ─────────────────────────────────────────────
st.markdown("""
<style>
/* ── Base font & background ──────────────────────────────────────────────── */
html, body, [class*="css"] {
    font-size: 18px !important;
    font-family: 'Segoe UI', Arial, sans-serif !important;
    background-color: #F7F9FC !important;
    color: #1A1A2E !important;
}

/* ── Sidebar ─────────────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background-color: #1A1A2E !important;
    color: #FFFFFF !important;
    padding: 1.5rem 1rem !important;
}
section[data-testid="stSidebar"] * {
    color: #FFFFFF !important;
    font-size: 18px !important;
}
section[data-testid="stSidebar"] .stCheckbox label {
    font-size: 18px !important;
    font-weight: 500;
}

/* ── Main headings ───────────────────────────────────────────────────────── */
h1 { font-size: 2rem !important; font-weight: 700; color: #1A1A2E !important; }
h2 { font-size: 1.6rem !important; font-weight: 600; color: #1A1A2E !important; }
h3 { font-size: 1.3rem !important; font-weight: 600; color: #1A1A2E !important; }

/* ── Job card container ──────────────────────────────────────────────────── */
.job-card {
    background-color: #FFFFFF;
    border: 2px solid #D0D8E8;
    border-left: 6px solid #0057B8;
    border-radius: 10px;
    padding: 1.4rem 1.6rem;
    margin-bottom: 1.2rem;
    box-shadow: 0 2px 6px rgba(0,0,0,0.06);
}
.job-card:hover {
    border-left-color: #003D82;
    box-shadow: 0 4px 14px rgba(0,87,184,0.12);
}

/* ── Job title ───────────────────────────────────────────────────────────── */
.job-title {
    font-size: 1.25rem !important;
    font-weight: 700;
    color: #1A1A2E;
    margin-bottom: 0.3rem;
}

/* ── Meta row (company · location · source) ──────────────────────────────── */
.job-meta {
    font-size: 1rem !important;
    color: #4A5568;
    margin-bottom: 0.6rem;
    line-height: 1.6;
}
.job-meta strong { color: #1A1A2E; }

/* ── Salary badge ────────────────────────────────────────────────────────── */
.salary-badge {
    display: inline-block;
    background-color: #E8F4E8;
    color: #1B5E20;
    border: 1px solid #A5D6A7;
    border-radius: 20px;
    padding: 3px 14px;
    font-size: 0.95rem !important;
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
    font-size: 0.95rem !important;
    margin-bottom: 0.8rem;
}

/* ── Region tag ──────────────────────────────────────────────────────────── */
.region-tag {
    display: inline-block;
    background-color: #EFF6FF;
    color: #1D4ED8;
    border: 1px solid #BFDBFE;
    border-radius: 20px;
    padding: 3px 12px;
    font-size: 0.9rem !important;
    font-weight: 600;
    margin-left: 8px;
}

/* ── Apply button ────────────────────────────────────────────────────────── */
.apply-btn {
    display: inline-block;
    background-color: #0057B8;
    color: #FFFFFF !important;
    font-size: 1.05rem !important;
    font-weight: 700;
    padding: 12px 28px;
    border-radius: 8px;
    text-decoration: none !important;
    border: 3px solid transparent;
    cursor: pointer;
    margin-top: 0.6rem;
    transition: none;   /* no animation — senior-friendly */
    letter-spacing: 0.3px;
}
.apply-btn:hover {
    background-color: #003D82;
    border-color: #001F4D;
    color: #FFFFFF !important;
}
.apply-btn:focus {
    outline: 4px solid #F59E0B;   /* high-visibility focus ring */
    outline-offset: 3px;
}

/* ── No results banner ───────────────────────────────────────────────────── */
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

/* ── Stats bar ───────────────────────────────────────────────────────────── */
.stats-bar {
    background-color: #EFF6FF;
    border: 1px solid #BFDBFE;
    border-radius: 8px;
    padding: 0.8rem 1.2rem;
    margin-bottom: 1.4rem;
    font-size: 1rem !important;
    color: #1D4ED8;
    font-weight: 600;
}

/* ── Dividers ────────────────────────────────────────────────────────────── */
hr { border-color: #E2E8F0 !important; margin: 1rem 0; }

/* ── Streamlit widget labels ─────────────────────────────────────────────── */
.stSlider label, .stCheckbox label, .stSelectbox label {
    font-size: 18px !important;
    font-weight: 600 !important;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# MOCK DATA — replace with real aggregator output
# Swap this block with:
#   df = asyncio.run(fetch_jobs())
#   df["final_url"] = df["url"].apply(get_direct_link)
# ─────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_mock_data() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "title":       "Senior Piping Engineer",
            "company":     "Shell Global Solutions",
            "location":    "Aberdeen, UK",
            "region":      "UK",
            "salary":      "75000 – 95000",
            "salary_num":  75000,
            "source":      "Adzuna",
            "final_url":   "https://careers.shell.com/",
            "scraped_at":  "2026-03-03",
        },
        {
            "title":       "Lead Piping Engineer",
            "company":     "Petrofac",
            "location":    "Aberdeen, UK",
            "region":      "UK",
            "salary":      "80000 – 100000",
            "salary_num":  80000,
            "source":      "Jooble",
            "final_url":   "https://www.petrofac.com/careers/",
            "scraped_at":  "2026-03-03",
        },
        {
            "title":       "Principal Piping Engineer",
            "company":     "Worley",
            "location":    "Mumbai, India",
            "region":      "India",
            "salary":      "N/A",
            "salary_num":  0,
            "source":      "Jooble",
            "final_url":   "https://www.worley.com/careers",
            "scraped_at":  "2026-03-03",
        },
        {
            "title":       "Senior Piping Design Engineer",
            "company":     "Engineers India Ltd",
            "location":    "New Delhi, India",
            "region":      "India",
            "salary":      "2800000 – 3500000",
            "salary_num":  2800000,
            "source":      "Adzuna",
            "final_url":   "https://www.engineersindia.com/career",
            "scraped_at":  "2026-03-03",
        },
        {
            "title":       "Lead Piping Engineer – LNG",
            "company":     "TechnipFMC",
            "location":    "Singapore",
            "region":      "Singapore",
            "salary":      "9000 – 12000 SGD",
            "salary_num":  9000,
            "source":      "Jooble",
            "final_url":   "https://www.technipfmc.com/en/careers/",
            "scraped_at":  "2026-03-03",
        },
        {
            "title":       "Senior Piping Engineer – FPSO",
            "company":     "McDermott International",
            "location":    "Kuala Lumpur, Malaysia",
            "region":      "Malaysia",
            "salary":      "N/A",
            "salary_num":  0,
            "source":      "Jooble",
            "final_url":   "https://www.mcdermott.com/Careers",
            "scraped_at":  "2026-03-03",
        },
        {
            "title":       "Principal Piping Engineer",
            "company":     "Sapura Energy",
            "location":    "Kuala Lumpur, Malaysia",
            "region":      "Malaysia",
            "salary":      "15000 – 20000 MYR",
            "salary_num":  15000,
            "source":      "Adzuna",
            "final_url":   "https://www.sapuraenergy.com/careers",
            "scraped_at":  "2026-03-03",
        },
        {
            "title":       "Senior Piping Engineer – Offshore",
            "company":     "Saudi Aramco",
            "location":    "Dhahran, Saudi Arabia",
            "region":      "Gulf",
            "salary":      "N/A",
            "salary_num":  0,
            "source":      "Jooble",
            "final_url":   "https://www.aramco.com/en/careers",
            "scraped_at":  "2026-03-03",
        },
        {
            "title":       "Lead Piping Engineer – Refinery",
            "company":     "ADNOC",
            "location":    "Abu Dhabi, UAE",
            "region":      "Gulf",
            "salary":      "35000 – 45000 AED",
            "salary_num":  35000,
            "source":      "Jooble",
            "final_url":   "https://careers.adnoc.ae/",
            "scraped_at":  "2026-03-03",
        },
        {
            "title":       "Principal Process & Piping Engineer",
            "company":     "QatarEnergy",
            "location":    "Doha, Qatar",
            "region":      "Gulf",
            "salary":      "N/A",
            "salary_num":  0,
            "source":      "Jooble",
            "final_url":   "https://careers.qatarenergy.qa/",
            "scraped_at":  "2026-03-03",
        },
    ])


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
REGION_FLAGS = {
    "UK":        "🇬🇧",
    "India":     "🇮🇳",
    "Singapore": "🇸🇬",
    "Malaysia":  "🇲🇾",
    "Gulf":      "🌍",
}

def render_job_card(row: pd.Series) -> None:
    """Render a single accessible, senior-friendly job card."""
    flag       = REGION_FLAGS.get(row["region"], "🌐")
    salary_str = str(row.get("salary", "N/A")).strip()
    has_salary = salary_str not in ("N/A", "", "0", "nan")
    salary_html = (
        f'<span class="salary-badge">💰 {salary_str}</span>'
        if has_salary else
        '<span class="salary-na">Salary not disclosed</span>'
    )
    final_url = str(row.get("final_url", "#")).strip()

    st.markdown(f"""
    <div class="job-card">
        <div class="job-title">
            🔧 {row['title']}
            <span class="region-tag">{flag} {row['region']}</span>
        </div>
        <div class="job-meta">
            <strong>🏢 Company:</strong> {row['company']}&nbsp;&nbsp;|&nbsp;&nbsp;
            <strong>📍 Location:</strong> {row['location']}&nbsp;&nbsp;|&nbsp;&nbsp;
            <strong>🔎 Source:</strong> {row['source']}
        </div>
        {salary_html}
        <br/>
        <a class="apply-btn"
           href="{final_url}"
           target="_blank"
           rel="noopener noreferrer"
           aria-label="Apply for {row['title']} at {row['company']} — opens in new tab">
            🔗 Apply on Company Website
        </a>
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔧 Filter Jobs")
    st.markdown("---")

    st.markdown("### 🌍 Regions")
    all_regions = ["UK", "India", "Singapore", "Malaysia", "Gulf"]
    selected_regions = []
    for region in all_regions:
        flag = REGION_FLAGS.get(region, "🌐")
        checked = st.checkbox(f"{flag}  {region}", value=True, key=f"region_{region}")
        if checked:
            selected_regions.append(region)

    st.markdown("---")
    st.markdown("### 💰 Minimum Salary")
    st.caption("ℹ️ Slide to filter. Jobs with undisclosed salary are always shown.")
    min_salary = st.slider(
        label="Minimum salary value",
        min_value=0,
        max_value=100000,
        value=0,
        step=5000,
        format="%d",
        label_visibility="collapsed",
    )
    st.markdown(f"**Set minimum:** `{min_salary:,}`")

    st.markdown("---")
    st.markdown("### 🔍 Search Title")
    title_search = st.text_input(
        "Filter by keyword in title",
        placeholder="e.g. FPSO, Offshore, LNG",
        label_visibility="visible",
    )

    st.markdown("---")
    st.markdown("### 📅 Sort By")
    sort_option = st.selectbox(
        "Sort listings by",
        options=["Most Recent", "Company A–Z", "Salary (High to Low)"],
        index=0,
        label_visibility="visible",
    )

    st.markdown("---")
    refresh = st.button("🔄 Refresh Job Listings", use_container_width=True)


# ─────────────────────────────────────────────
# MAIN CONTENT
# ─────────────────────────────────────────────
st.markdown("# 🔧 Senior Piping Engineer — Job Listings")
st.markdown(
    "Showing **Senior**, **Lead**, and **Principal** roles · "
    "Minimum **20 years experience** · "
    "Sourced from Adzuna & Jooble"
)
st.markdown("---")

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading job listings..."):
    df = load_mock_data()
    if refresh:
        st.cache_data.clear()
        df = load_mock_data()

# ── Apply filters ─────────────────────────────────────────────────────────────
filtered = df.copy()

# Region filter
if selected_regions:
    filtered = filtered[filtered["region"].isin(selected_regions)]
else:
    st.warning("⚠️ Please select at least one region in the sidebar.")
    st.stop()

# Salary filter — keep N/A rows (salary_num == 0) always visible
salary_mask = (filtered["salary_num"] == 0) | (filtered["salary_num"] >= min_salary)
filtered = filtered[salary_mask]

# Title keyword filter
if title_search.strip():
    filtered = filtered[
        filtered["title"].str.contains(title_search.strip(), case=False, na=False)
    ]

# Sorting
if sort_option == "Company A–Z":
    filtered = filtered.sort_values("company")
elif sort_option == "Salary (High to Low)":
    filtered = filtered.sort_values("salary_num", ascending=False)
else:
    filtered = filtered.sort_values("scraped_at", ascending=False)

filtered = filtered.reset_index(drop=True)

# ── Stats bar ─────────────────────────────────────────────────────────────────
total         = len(filtered)
with_salary   = (filtered["salary_num"] > 0).sum()
region_counts = " · ".join(
    f"{REGION_FLAGS.get(r,'🌐')} {r}: {n}"
    for r, n in filtered["region"].value_counts().items()
)
st.markdown(f"""
<div class="stats-bar">
    📋 <strong>{total} job{'' if total == 1 else 's'} found</strong>
    &nbsp;|&nbsp;
    💰 {with_salary} with salary disclosed
    &nbsp;|&nbsp;
    {region_counts}
</div>
""", unsafe_allow_html=True)

# ── Job cards ─────────────────────────────────────────────────────────────────
if filtered.empty:
    st.markdown("""
    <div class="no-results">
        <h3>😔 No jobs match your current filters.</h3>
        <p>Try selecting more regions, lowering the salary slider, or clearing the title search.</p>
    </div>
    """, unsafe_allow_html=True)
else:
    for _, row in filtered.iterrows():
        render_job_card(row)

# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#9CA3AF; font-size:0.9rem;'>"
    "Data sourced from Adzuna & Jooble APIs · "
    "Links resolved via job_link_resolver · "
    "Listings refreshed daily"
    "</div>",
    unsafe_allow_html=True,
)
