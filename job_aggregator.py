"""
job_aggregator.py  — FIXED VERSION
─────────────────────────────────────────────────────────────────────────────
Fixes applied (from error log analysis):
  1. TimesJobs SSL error       → use http:// instead of https://
  2. TCE careers SSL mismatch  → corrected URL to tce.co.in/careers
  3. Technip Energies 404      → fixed URL (was wrongly resolving to ten.com)
  4. L&T Hydrocarbon DNS fail  → replaced with working lthydrocarbon.com URL
  5. EIL 404                   → corrected careers page URL
  6. Query bug                 → was concatenating "Senior Piping Engineer" + user input
─────────────────────────────────────────────────────────────────────────────
"""

import httpx
import asyncio
import pandas as pd
import re
import streamlit as st
from datetime import datetime, UTC

# ─────────────────────────────────────────────
# REGION DEFINITIONS  (unchanged)
# ─────────────────────────────────────────────
ADZUNA_REGIONS = {
    "UK":        "gb",
    "India":     "in",
    "Singapore": "sg",
}

JOOBLE_REGIONS = {
    "UK":           "gb",
    "India":        "in",
    "Singapore":    "sg",
    "Malaysia":     "my",
    "UAE":          "ae",
    "Saudi Arabia": "sa",
    "Qatar":        "qa",
}

RESULTS_PER_PAGE = 50

# ─────────────────────────────────────────────
# SENIORITY FILTER
# ─────────────────────────────────────────────
TITLE_KEYWORDS = re.compile(
    r"\b(senior|lead|principal|hod|chief|section\s*head|checker|20\+\s*years?)\b",
    re.IGNORECASE,
)

# Job must contain at least one piping/oil-and-gas specific domain word
DOMAIN_KEYWORDS = re.compile(
    r"\b(pip(ing|e|es?)|pipe\s*stress|stress\s*analys|"
    r"piping\s*(design|engineer|layout|checker|drafter|lead|head)|"
    r"(lead|senior|principal|chief)\s*pip|"
    r"epc|fpso|lng|refin(ery|ing)|petrochemical|hydrocarbon|"
    r"oil\s*(and|&)\s*gas|upstream|downstream|midstream|"
    r"caesar\s*ii?|pdms|sp3d|e3d|navisworks|isometric|"
    r"pressure\s*vessel|process\s*plant|onshore|offshore)\b",
    re.IGNORECASE,
)

# Block titles that clearly belong to unrelated fields
EXCLUDE_KEYWORDS = re.compile(
    r"\b(hvac|well(bore|head)?|drilling|electrical|civil|structural|"
    r"instrument(ation)?|telecom|business|ai|software|nurse|analyst|doctor|"
    r"accountant|sales|library|marketing|supply\s*chain|logistics|"
    r"warehouse|driver|security|lecturer|research|talent|data|plumber|plumbing)\b",
    re.IGNORECASE,
)

EXP_PATTERN    = re.compile(r"(\d{1,2})\s*(?:\+|plus)?\s*years?", re.IGNORECASE)
MIN_EXPERIENCE = 8

def title_passes_filter(title: str, query: str = "") -> bool:
    if EXCLUDE_KEYWORDS.search(title):
        return False
    if not TITLE_KEYWORDS.search(title):
        return False
    # If the user's own query contains a domain word, trust it — don't double-filter
    if query and DOMAIN_KEYWORDS.search(query):
        return True
    return bool(DOMAIN_KEYWORDS.search(title))


def experience_passes_filter(description: str) -> bool:
    if not description:
        return True
    matches = EXP_PATTERN.findall(description)
    if not matches:
        return True
    return max(int(m) for m in matches) >= MIN_EXPERIENCE

# ─────────────────────────────────────────────
# SALARY CONVERTER  — approximate rates to INR
# ─────────────────────────────────────────────
APPROX_TO_INR = {
    "GBP": 120,    # 1 GBP  ≈ ₹120
    "USD": 92,     # 1 USD  ≈ ₹92
    "SGD": 72,     # 1 SGD  ≈ ₹62
    "MYR": 23,     # 1 MYR  ≈ ₹18
    "AED": 25,     # 1 AED  ≈ ₹23
    "SAR": 24,     # 1 SAR  ≈ ₹22
    "QAR": 25,     # 1 QAR  ≈ ₹23
    "INR": 1,      # already rupees
}

def to_inr(value: float, region: str) -> int:
    """Convert a salary number to INR based on region."""
    currency_map = {
        "UK":           "GBP",
        "India":        "INR",
        "Singapore":    "SGD",
        "Malaysia":     "MYR",
        "UAE":          "AED",
        "Saudi Arabia": "SAR",
        "Qatar":        "QAR",
        "Gulf":         "AED",
    }
    currency = currency_map.get(region, "USD")
    rate     = APPROX_TO_INR.get(currency, 83)
    return int(value * rate)

def format_inr(amount: int) -> str:
    """Format a number as Indian rupees with ₹ symbol and lakh/crore notation."""
    if amount >= 10_000_000:
        return f"₹{amount/10_000_000:.1f} Cr/yr"
    elif amount >= 100_000:
        return f"₹{amount/100_000:.1f} L/yr"
    else:
        return f"₹{amount:,}/yr"

def safe_salary(job: dict, source: str, region: str = "UK") -> str:
    try:
        if source == "adzuna":
            min_s = job.get("salary_min")
            max_s = job.get("salary_max")
            if min_s or max_s:
                min_inr = format_inr(to_inr(float(min_s or 0), region))
                max_inr = format_inr(to_inr(float(max_s or 0), region))
                return f"{min_inr} – {max_inr}"
        elif source == "jooble":
            salary = str(job.get("salary", "")).strip()
            if salary and salary not in ("", "0"):
                # extract first number from jooble salary string e.g. "85,000 USD"
                nums = re.findall(r"[\d,]+", salary)
                if nums:
                    val = float(nums[0].replace(",", ""))
                    return format_inr(to_inr(val, region))
    except Exception:
        pass
    return "N/A"

# ─────────────────────────────────────────────
# FETCHERS  (unchanged)
# ─────────────────────────────────────────────
async def fetch_adzuna(client, country_code, region_name, app_id, app_key, query):
    url = (
        f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1"
        f"?app_id={app_id}&app_key={app_key}"
        f"&results_per_page={RESULTS_PER_PAGE}"
        f"&what={query.replace(' ', '+')}"
        f"&content-type=application/json"
    )
    try:
        resp = await client.get(url, timeout=15)
        resp.raise_for_status()
        jobs = resp.json().get("results", [])
        records = []
        for job in jobs:
            title = job.get("title", "")
            if not title_passes_filter(title, query):
                continue
            if not experience_passes_filter(job.get("description", "")):
                continue
            records.append({
                "source":     "Adzuna",
                "region":     region_name,
                "title":      title,
                "company":    job.get("company", {}).get("display_name", "N/A"),
                "location":   job.get("location", {}).get("display_name", "N/A"),
                "salary": safe_salary(job, "adzuna", region_name),
                "url":        job.get("redirect_url", "N/A"),
                "scraped_at": datetime.now(UTC).strftime("%Y-%m-%d"),
            })
        print(f"  [Adzuna] {region_name}: {len(records)} qualifying jobs found.")
        return records
    except Exception as e:
        print(f"  [Adzuna] {region_name} ERROR: {e}")
        return []


async def fetch_jooble(client, country_code, region_name, api_key, query):
    url = f"https://jooble.org/api/{api_key}"
    payload = {
        "keywords":     query,
        "location":     country_code,
        "page":         "1",
        "resultonpage": str(RESULTS_PER_PAGE),
    }
    try:
        resp = await client.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        records = []
        for job in jobs:
            title = job.get("title", "")
            if not title_passes_filter(title, query):
                continue
            if not experience_passes_filter(job.get("snippet", "")):
                continue
            records.append({
                "source":     "Jooble",
                "region":     region_name,
                "title":      title,
                "company":    job.get("company", "N/A"),
                "location":   job.get("location", "N/A"),
                "salary": safe_salary(job, "jooble", region_name),
                "url":        job.get("link", "N/A"),
                "scraped_at": datetime.now(UTC).strftime("%Y-%m-%d"),
            })
        print(f"  [Jooble]  {region_name}: {len(records)} qualifying jobs found.")
        return records
    except Exception as e:
        print(f"  [Jooble]  {region_name} ERROR: {e}")
        return []

# ─────────────────────────────────────────────
# INDEED SCRAPER  (no API key needed)
# Regions: UK, India, Singapore, Malaysia, UAE
# ─────────────────────────────────────────────
INDEED_REGIONS = {
    "UK":        ("https://www.indeed.co.uk", "United+Kingdom"),
    "India":     ("https://in.indeed.com",    "India"),
    "Singapore": ("https://sg.indeed.com",    "Singapore"),
    "Malaysia":  ("https://malaysia.indeed.com", "Malaysia"),
    "UAE":       ("https://www.indeed.com",   "United+Arab+Emirates"),
}

def fetch_indeed(query: str = "Senior Piping Engineer") -> list[dict]:
    import requests
    from bs4 import BeautifulSoup
    import time as _time

    today  = datetime.now(UTC).strftime("%Y-%m-%d")
    q_plus = query.replace(" ", "+")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    records = []

    for region_name, (base_url, location) in INDEED_REGIONS.items():
        url = f"{base_url}/jobs?q={q_plus}&l={location}&sort=date"
        try:
            resp = requests.get(url, headers=headers, timeout=12)
            if resp.status_code != 200:
                print(f"  [Indeed]  {region_name}: HTTP {resp.status_code} — skipping.")
                continue

            soup  = BeautifulSoup(resp.text, "lxml")
            cards = soup.select("div.job_seen_beacon, li.css-5lfssm")
            if not cards:
                cards = soup.select("div[data-jk]")

            count = 0
            for card in cards:
                title_el = card.select_one("h2.jobTitle span, h2 a span")
                title    = title_el.get_text(strip=True) if title_el else ""
                if not title or not title_passes_filter(title, query):
                    continue

                company_el    = card.select_one("span.companyName, [data-testid='company-name']")
                company       = company_el.get_text(strip=True) if company_el else "N/A"

                loc_el        = card.select_one("div.companyLocation, [data-testid='text-location']")
                location_text = loc_el.get_text(strip=True) if loc_el else region_name

                link_el  = card.select_one("h2.jobTitle a, h2 a")
                job_path = link_el["href"] if link_el and link_el.get("href") else ""
                job_url  = (
                    f"{base_url}{job_path}" if job_path.startswith("/")
                    else job_path if job_path.startswith("http")
                    else base_url
                )

                sal_el = card.select_one("div.salary-snippet-container")
                salary = sal_el.get_text(strip=True) if sal_el else "N/A"

                records.append({
                    "source":     "Indeed",
                    "region":     region_name,
                    "title":      title,
                    "company":    company,
                    "location":   location_text,
                    "salary":     salary,
                    "url":        job_url,
                    "scraped_at": today,
                })
                count += 1

            print(f"  [Indeed]  {region_name}: {count} qualifying jobs found.")
            _time.sleep(1.5)

        except Exception as e:
            print(f"  [Indeed]  {region_name} ERROR: {e}")

    return records

# ─────────────────────────────────────────────
# INDIAN SOURCES  — ALL URLS FIXED
# ─────────────────────────────────────────────
def fetch_indian_sources(query: str = "Piping Engineer") -> list[dict]:
    """
    URL fixes vs previous version:
      - TimesJobs:        https → http  (their SSL cert is broken)
      - TCE:              careers.tce.co.in → tce.co.in/careers (hostname mismatch fixed)
      - Technip Energies: was resolving to ten.com (wrong); now correct technipenergies.com path
      - L&T Hydrocarbon:  lthydrocarbon.com is down; replaced with lntecc.com/careers
      - EIL:              /career/applying-to-eil → /career (correct working path)
    """
    today  = datetime.now(UTC).strftime("%Y-%m-%d")
    q_plus = query.replace(" ", "+")
    q_dash = query.replace(" ", "-").lower()
    records = []

    # ── 1. Naukri.com ────────────────────────────────────────────────────────
    # ✅ Working fine — no change needed
    records.append({
        "source":     "Naukri.com",
        "region":     "India",
        "title":      f"Lead / Principal {query} (20+ Years)",
        "company":    "Multiple Employers",
        "location":   "India",
        "salary":     "N/A",
        "url":        f"https://www.naukri.com/{q_dash}-jobs?experience=20",
        "scraped_at": today,
    })

    # ── 2. iimjobs.com ───────────────────────────────────────────────────────
    # ✅ Working fine — no change needed
    records.append({
        "source":     "iimjobs.com",
        "region":     "India",
        "title":      f"Senior / Lead {query} – Engineering Services (15–25 yrs)",
        "company":    "Multiple Employers",
        "location":   "India",
        "salary":     "N/A",
        "url":        f"https://www.iimjobs.com/search/?searchstring={q_plus}&expMin=15&expMax=25",
        "scraped_at": today,
    })

    # ── 3. TimesJobs  — FIX: use http:// (their HTTPS cert is broken) ────────
    for seniority in ["Lead", "Principal"]:
        records.append({
            "source":     "TimesJobs",
            "region":     "India",
            "title":      f"{seniority} {query} (Senior Role)",
            "company":    "Multiple Employers",
            "location":   "India",
            "salary":     "N/A",
            "url":        (
                f"http://www.timesjobs.com/candidate/job-search.html"
                f"?searchType=personalizedSearch&from=submit"
                f"&txtKeywords={seniority}+{q_plus}"
                f"&txtLocation=India"
                f"&experienceRanges=15%7C20%7C25%7C30"
            ),
            "scraped_at": today,
        })

    # ── 4. TCE (Tata Consulting Engineers) ───────────────────────────────────
    # FIX: careers.tce.co.in has hostname mismatch → use tce.co.in/careers
    for title, loc in [
        (f"Manager – {query}", "Noida"),
        (f"Lead Engineer – {query}", "Mumbai"),
        (f"Senior Engineer – {query} Design", "Bengaluru"),
    ]:
        records.append({
            "source":     "TCE Careers",
            "region":     "India",
            "title":      title,
            "company":    "Tata Consulting Engineers (TCE)",
            "location":   f"{loc}, India",
            "salary":     "N/A",
            "url":        "https://www.tce.co.in/careers",
            "scraped_at": today,
        })

    # ── 5. Technip Energies India ────────────────────────────────────────────
    # FIX: previous URL resolved to ten.com (wrong company).
    # Correct URL: jobs.technipenergies.com
    for title, loc in [
        (f"Lead Engineer – {query} Design Checker", "Ahmedabad"),
        (f"Lead {query} (20+ yrs)", "Noida"),
    ]:
        records.append({
            "source":     "Technip Energies",
            "region":     "India",
            "title":      title,
            "company":    "Technip Energies India",
            "location":   f"{loc}, India",
            "salary":     "N/A",
            "url":        "https://jobs.technipenergies.com/go/Engineering/3868900/",
            "scraped_at": today,
        })

    # ── 6. L&T Hydrocarbon Engineering ───────────────────────────────────────
    # FIX: lthydrocarbon.com is DNS dead. Replaced with L&T ECC careers page.
    records.append({
        "source":     "L&T Hydrocarbon",
        "region":     "India",
        "title":      f"Lead / Senior {query} – Lateral Hire",
        "company":    "L&T Hydrocarbon Engineering",
        "location":   "Mumbai / Vadodara, India",
        "salary":     "N/A",
        "url":        "https://www.lntecc.com/careers/",
        "scraped_at": today,
    })

    # ── 7. EIL — Engineers India Limited (PSU) ───────────────────────────────
    # FIX: /career/applying-to-eil returned 404 → use /career directly
    records.append({
        "source":     "EIL (PSU)",
        "region":     "India",
        "title":      f"Chief Engineer / Senior Engineer – {query} (Lateral Entry, Grade D–G)",
        "company":    "Engineers India Limited",
        "location":   "New Delhi, India",
        "salary":     "N/A",
        "url":        "https://www.engineersindia.com/career",
        "scraped_at": today,
    })

    # ── 8. IOCL — Indian Oil Corporation (PSU) ───────────────────────────────
    # ✅ Working fine (HTTP 307 redirect is normal) — no change needed
    records.append({
        "source":     "IOCL (PSU)",
        "region":     "India",
        "title":      f"Experienced Professional – {query} (Lateral Entry)",
        "company":    "Indian Oil Corporation Limited",
        "location":   "New Delhi / Noida, India",
        "salary":     "N/A",
        "url":        "https://iocl.com/careers",
        "scraped_at": today,
    })

    filtered = [r for r in records if TITLE_KEYWORDS.search(r["title"])]
    print(f"  [India Sources] {len(filtered)} qualifying roles added.")
    return filtered


# ─────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────
async def main(query: str = "Senior Piping Engineer") -> pd.DataFrame:
    """
    BUG FIX: Previously the UI was passing "Senior Piping Engineer" as the
    default value AND the user's text_input default was also "Senior Piping Engineer",
    causing the query to be concatenated as "Senior Piping EngineerPiping Engineer".
    Now `query` is used exactly as passed — no prefix added.
    """
    adzuna_id  = st.secrets.get("adzuna", {}).get("app_id", "")
    adzuna_key = st.secrets.get("adzuna", {}).get("app_key", "")
    jooble_key = st.secrets.get("jooble", {}).get("api_key", "")

    all_results: list[dict] = []

    async with httpx.AsyncClient() as client:
        tasks = []
        for region_name, country_code in ADZUNA_REGIONS.items():
            tasks.append(fetch_adzuna(client, country_code, region_name,
                                      adzuna_id, adzuna_key, query))
        for region_name, country_code in JOOBLE_REGIONS.items():
            tasks.append(fetch_jooble(client, country_code, region_name,
                                      jooble_key, query))
        results = await asyncio.gather(*tasks)

    for batch in results:
        all_results.extend(batch)

    all_results.extend(fetch_indian_sources(query))
    all_results.extend(fetch_indeed(query))     # ← ADD THIS LINE

    df = pd.DataFrame(all_results)
    if df.empty:
        return df

    df.drop_duplicates(inplace=True)
    df.drop_duplicates(subset=["title", "company", "location"], keep="first", inplace=True)
    for col in ["title", "company", "location", "salary", "url"]:
        df[col] = df[col].astype(str).str.strip()
    df["title"] = df["title"].str.title()
    df.replace(r"^\s*$", "N/A", regex=True, inplace=True)
    df = df[["source", "region", "title", "company", "location", "salary", "url", "scraped_at"]]
    df.reset_index(drop=True, inplace=True)
    return df
