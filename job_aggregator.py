"""
job_aggregator.py
─────────────────────────────────────────────────────────────────────────────
Senior Engineer Job Aggregator — Full Version
New in this version:
  - PRIORITY_COMPANIES set (boost + star badge for known EPC/OG companies)
  - PRIORITY_JOB_SITES curated link cards (Rigzone, NaukriGulf, Airswift etc.)
  - relevance_rank() for piping-first sorting
  - All existing features preserved
─────────────────────────────────────────────────────────────────────────────
"""

import httpx
import asyncio
import pandas as pd
import re
import streamlit as st
from datetime import datetime, UTC

# ─────────────────────────────────────────────
# REGION DEFINITIONS
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
# PRIORITY COMPANIES  — EPC / Oil & Gas / OEM
# Jobs from these companies get a ⭐ badge and are boosted to the top
# ─────────────────────────────────────────────
PRIORITY_COMPANIES = {
    # EPC / Engineering Consultancies
    "larsen & toubro", "l&t", "tata projects", "tata consulting engineers", "tce",
    "engineers india limited", "eil", "worley", "technip energies", "technipfmc",
    "wood plc", "wood group", "kbr", "mcdermott", "saipem", "bechtel",
    "samsung engineering", "aker solutions", "tecnimont", "uhde india", "thyssenkrupp",
    "afcons", "hindustan construction", "hcc", "gmr infrastructure", "ircon",
    "meil", "megha engineering", "kalpataru", "kptl", "kec international",
    "purna design", "global hi-tech", "quest global", "air liquide",
    "bansal infratech", "buildcraft", "enventure", "sunshine workforce",
    "aarvi encon", "lamprell", "va tech wabag", "valmet", "mott macdonald",
    "jacobs", "penspen", "stantec", "atkinsrealis", "atkins", "babcock",
    "national gas", "exxonmobil", "mn dastur", "m.n. dastur", "proton engineering",
    "chempro", "esteem projects", "yokogawa", "petronet lng", "gail", "ongc",
    "reliance", "adani", "mie industrial", "oceanmight", "avanceon",
    "framatome", "aecom", "wsp", "costain", "bae systems",
    # Staffing / Recruitment specialists in oil & gas
    "airswift", "nes fircroft", "orion group", "trs staffing", "transcend hr",
    "perito", "jackson hogg", "scantec", "wolviston", "highfield professional",
    "carmichael", "ata recruitment", "adepto", "emco talent",
    "rise technical", "henderson brown", "ernest gordon", "alecto",
    "sterling choice", "apex resourcing", "chrysalis talent", "cv technical",
    "plan recruit", "silicon valley associates",
    # O&G operators
    "saudi aramco", "adnoc", "qatarenergy", "shell", "bp", "totalenergies",
    "chevron", "conocophillips", "halliburton", "schlumberger", "slb",
}

# ─────────────────────────────────────────────
# SENIORITY FILTER
# ─────────────────────────────────────────────
TITLE_KEYWORDS = re.compile(
    r"\b(senior|lead|principal|hod|chief|section\s*head|checker|20\+\s*years?)\b",
    re.IGNORECASE,
)

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

# Explicitly piping — these jobs go to the very top (rank 0)
PIPING_EXPLICIT = re.compile(
    r"\b(pip(ing|e|es?)|pipe\s*stress|stress\s*analys|caesar|pdms|sp3d|isometric)\b",
    re.IGNORECASE,
)

EXCLUDE_KEYWORDS = re.compile(
    r"\b(hvac|well(bore|head)?|drilling|electrical|civil|structural|"
    r"instrument(ation)?|telecom|software|nurse|doctor|"
    r"accountant|sales|marketing|supply\s*chain|logistics|"
    r"warehouse|driver|security|plumber|plumbing)\b",
    re.IGNORECASE,
)

EXP_PATTERN    = re.compile(r"(\d{1,2})\s*(?:\+|plus)?\s*years?", re.IGNORECASE)
MIN_EXPERIENCE = 8


def title_passes_filter(title: str, query: str = "") -> bool:
    if EXCLUDE_KEYWORDS.search(title):
        return False
    if not TITLE_KEYWORDS.search(title):
        return False
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


def relevance_rank(title: str) -> int:
    """
    0 = explicitly piping/stress (shown first)
    1 = domain-related but not explicitly piping
    2 = seniority match only
    """
    if PIPING_EXPLICIT.search(title):
        return 0
    if DOMAIN_KEYWORDS.search(title):
        return 1
    return 2


def is_priority_company(company: str) -> bool:
    return company.lower().strip() in PRIORITY_COMPANIES or any(
        p in company.lower() for p in PRIORITY_COMPANIES
    )


# ─────────────────────────────────────────────
# SALARY CONVERTER
# ─────────────────────────────────────────────
APPROX_TO_INR = {
    "GBP": 120, "USD": 92, "SGD": 72,
    "MYR": 23,  "AED": 25, "SAR": 24,
    "QAR": 25,  "INR": 1,
}

def to_inr(value: float, region: str) -> int:
    currency_map = {
        "UK": "GBP", "India": "INR", "Singapore": "SGD",
        "Malaysia": "MYR", "UAE": "AED", "Saudi Arabia": "SAR",
        "Qatar": "QAR", "Gulf": "AED",
    }
    rate = APPROX_TO_INR.get(currency_map.get(region, "USD"), 92)
    return int(value * rate)

def format_inr(amount: int) -> str:
    if amount >= 10_000_000:
        return f"\u20b9{amount/10_000_000:.1f} Cr/yr"
    elif amount >= 100_000:
        return f"\u20b9{amount/100_000:.1f} L/yr"
    return f"\u20b9{amount:,}/yr"

def safe_salary(job: dict, source: str, region: str = "UK") -> str:
    try:
        if source == "adzuna":
            min_s = job.get("salary_min")
            max_s = job.get("salary_max")
            if min_s or max_s:
                return f"{format_inr(to_inr(float(min_s or 0), region))} – {format_inr(to_inr(float(max_s or 0), region))}"
        elif source == "jooble":
            salary = str(job.get("salary", "")).strip()
            if salary and salary not in ("", "0"):
                nums = re.findall(r"[\d,]+", salary)
                if nums:
                    return format_inr(to_inr(float(nums[0].replace(",", "")), region))
    except Exception:
        pass
    return "N/A"


# ─────────────────────────────────────────────
# FETCHERS
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
        records = []
        for job in resp.json().get("results", []):
            title = job.get("title", "")
            if not title_passes_filter(title, query):
                continue
            if not experience_passes_filter(job.get("description", "")):
                continue
            company = job.get("company", {}).get("display_name", "N/A")
            records.append({
                "source":     "Adzuna",
                "region":     region_name,
                "title":      title,
                "company":    company,
                "location":   job.get("location", {}).get("display_name", "N/A"),
                "salary":     safe_salary(job, "adzuna", region_name),
                "url":        job.get("redirect_url", "N/A"),
                "scraped_at": datetime.now(UTC).strftime("%Y-%m-%d"),
                "priority":   is_priority_company(company),
                "rank":       relevance_rank(title),
            })
        print(f"  [Adzuna] {region_name}: {len(records)} qualifying jobs found.")
        return records
    except Exception as e:
        print(f"  [Adzuna] {region_name} ERROR: {e}")
        return []


async def fetch_jooble(client, country_code, region_name, api_key, query):
    url = f"https://jooble.org/api/{api_key}"
    payload = {"keywords": query, "location": country_code, "page": "1", "resultonpage": str(RESULTS_PER_PAGE)}
    try:
        resp = await client.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        records = []
        for job in resp.json().get("jobs", []):
            title = job.get("title", "")
            if not title_passes_filter(title, query):
                continue
            if not experience_passes_filter(job.get("snippet", "")):
                continue
            company = job.get("company", "N/A")
            records.append({
                "source":     "Jooble",
                "region":     region_name,
                "title":      title,
                "company":    company,
                "location":   job.get("location", "N/A"),
                "salary":     safe_salary(job, "jooble", region_name),
                "url":        job.get("link", "N/A"),
                "scraped_at": datetime.now(UTC).strftime("%Y-%m-%d"),
                "priority":   is_priority_company(company),
                "rank":       relevance_rank(title),
            })
        print(f"  [Jooble]  {region_name}: {len(records)} qualifying jobs found.")
        return records
    except Exception as e:
        print(f"  [Jooble]  {region_name} ERROR: {e}")
        return []


# ─────────────────────────────────────────────
# INDEED SCRAPER
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
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
            cards = soup.select("div.job_seen_beacon, li.css-5lfssm") or soup.select("div[data-jk]")
            count = 0
            for card in cards:
                title_el = card.select_one("h2.jobTitle span, h2 a span")
                title    = title_el.get_text(strip=True) if title_el else ""
                if not title or not title_passes_filter(title, query):
                    continue
                company_el    = card.select_one("span.companyName, [data-testid=\'company-name\']")
                company       = company_el.get_text(strip=True) if company_el else "N/A"
                loc_el        = card.select_one("div.companyLocation, [data-testid=\'text-location\']")
                location_text = loc_el.get_text(strip=True) if loc_el else region_name
                link_el  = card.select_one("h2.jobTitle a, h2 a")
                job_path = link_el["href"] if link_el and link_el.get("href") else ""
                job_url  = (f"{base_url}{job_path}" if job_path.startswith("/") else job_path if job_path.startswith("http") else base_url)
                sal_el = card.select_one("div.salary-snippet-container")
                salary = sal_el.get_text(strip=True) if sal_el else "N/A"
                records.append({
                    "source": "Indeed", "region": region_name, "title": title,
                    "company": company, "location": location_text, "salary": salary,
                    "url": job_url, "scraped_at": today,
                    "priority": is_priority_company(company), "rank": relevance_rank(title),
                })
                count += 1
            print(f"  [Indeed]  {region_name}: {count} qualifying jobs found.")
            _time.sleep(1.5)
        except Exception as e:
            print(f"  [Indeed]  {region_name} ERROR: {e}")
    return records


# ─────────────────────────────────────────────
# INDIAN SOURCES + PRIORITY JOB SITES
# ─────────────────────────────────────────────
def fetch_indian_sources(query: str = "Piping Engineer") -> list[dict]:
    today  = datetime.now(UTC).strftime("%Y-%m-%d")
    q_plus = query.replace(" ", "+")
    q_dash = query.replace(" ", "-").lower()
    records = []

    # ── Core Indian job portals ──────────────────────────────────────────────
    records.append({"source": "Naukri.com", "region": "India",
        "title": f"Lead / Principal {query} (20+ Years)", "company": "Multiple Employers",
        "location": "India", "salary": "N/A",
        "url": f"https://www.naukri.com/{q_dash}-jobs?experience=20",
        "scraped_at": today, "priority": False, "rank": 0})

    records.append({"source": "iimjobs.com", "region": "India",
        "title": f"Senior / Lead {query} – Engineering Services (15–25 yrs)", "company": "Multiple Employers",
        "location": "India", "salary": "N/A",
        "url": f"https://www.iimjobs.com/search/?searchstring={q_plus}&expMin=15&expMax=25",
        "scraped_at": today, "priority": False, "rank": 0})

    records.append({"source": "Foundit", "region": "India",
        "title": f"Senior / Lead {query} (15+ yrs)", "company": "Multiple Employers",
        "location": "India", "salary": "N/A",
        "url": f"https://www.foundit.in/srp/results?searchId=&query={q_plus}&experienceRanges=15-20",
        "scraped_at": today, "priority": False, "rank": 0})

    records.append({"source": "Shine.com", "region": "India",
        "title": f"Lead / Principal {query}", "company": "Multiple Employers",
        "location": "India", "salary": "N/A",
        "url": f"https://www.shine.com/job-search/{q_dash}-jobs",
        "scraped_at": today, "priority": False, "rank": 0})

    records.append({"source": "Apna", "region": "India",
        "title": f"Senior {query} Openings", "company": "Multiple Employers",
        "location": "India", "salary": "N/A",
        "url": f"https://apna.co/jobs/{q_dash}",
        "scraped_at": today, "priority": False, "rank": 0})

    for seniority in ["Lead", "Principal"]:
        records.append({"source": "TimesJobs", "region": "India",
            "title": f"{seniority} {query} (Senior Role)", "company": "Multiple Employers",
            "location": "India", "salary": "N/A",
            "url": (f"http://www.timesjobs.com/candidate/job-search.html"
                    f"?searchType=personalizedSearch&from=submit"
                    f"&txtKeywords={seniority}+{q_plus}&txtLocation=India"
                    f"&experienceRanges=15%7C20%7C25%7C30"),
            "scraped_at": today, "priority": False, "rank": 0})

    # ── Gulf / International specialist boards ───────────────────────────────
    records.append({"source": "NaukriGulf", "region": "UAE",
        "title": f"Senior / Lead {query} – Gulf Region", "company": "Multiple Employers",
        "location": "Gulf", "salary": "N/A",
        "url": f"https://www.naukrigulf.com/{q_dash}-jobs",
        "scraped_at": today, "priority": False, "rank": 0})

    records.append({"source": "GulfTalent", "region": "UAE",
        "title": f"Lead {query} – UAE / Qatar / KSA", "company": "Multiple Employers",
        "location": "Gulf", "salary": "N/A",
        "url": f"https://www.gulftalent.com/jobs/keywords/{q_dash}",
        "scraped_at": today, "priority": False, "rank": 0})

    records.append({"source": "Bayt", "region": "UAE",
        "title": f"Principal / Senior {query} – Middle East", "company": "Multiple Employers",
        "location": "Gulf", "salary": "N/A",
        "url": f"https://www.bayt.com/en/international/jobs/{q_dash}-jobs/",
        "scraped_at": today, "priority": False, "rank": 0})

    records.append({"source": "Jobstreet", "region": "Malaysia",
        "title": f"Lead {query} – Malaysia / Singapore", "company": "Multiple Employers",
        "location": "Malaysia", "salary": "N/A",
        "url": f"https://www.jobstreet.com.my/en/job-search/{q_dash}-jobs/",
        "scraped_at": today, "priority": False, "rank": 0})

    # ── Oil & Gas specialist boards ──────────────────────────────────────────
    records.append({"source": "Rigzone", "region": "UK",
        "title": f"Senior / Lead {query} – Rigzone", "company": "Multiple Employers",
        "location": "Global", "salary": "N/A",
        "url": f"https://www.rigzone.com/oil/jobs/search/?k={q_plus}&t=2",
        "scraped_at": today, "priority": False, "rank": 0})

    records.append({"source": "EnergyJobSearch", "region": "UK",
        "title": f"Lead / Principal {query} – Energy Sector", "company": "Multiple Employers",
        "location": "Global", "salary": "N/A",
        "url": f"https://www.energyjobsearch.com/jobs/?keywords={q_plus}&experience=senior",
        "scraped_at": today, "priority": False, "rank": 0})

    records.append({"source": "OilCareers / Airswift", "region": "UK",
        "title": f"Senior {query} – Airswift Network", "company": "Airswift",
        "location": "Global", "salary": "N/A",
        "url": f"https://jobs.airswift.com/jobs/?keyword={q_plus}",
        "scraped_at": today, "priority": True, "rank": 0})

    records.append({"source": "NES Fircroft", "region": "UK",
        "title": f"Lead / Principal {query} – NES Fircroft", "company": "NES Fircroft",
        "location": "Global", "salary": "N/A",
        "url": f"https://www.nesfircroft.com/jobs?q={q_plus}",
        "scraped_at": today, "priority": True, "rank": 0})

    records.append({"source": "Orion Group", "region": "UK",
        "title": f"Senior {query} – Orion Group", "company": "Orion Group",
        "location": "Global", "salary": "N/A",
        "url": f"https://www.orioneng.com/jobs/?search={q_plus}",
        "scraped_at": today, "priority": True, "rank": 0})

    records.append({"source": "TRS Staffing", "region": "UK",
        "title": f"Lead {query} – TRS Staffing", "company": "TRS Staffing",
        "location": "Global", "salary": "N/A",
        "url": f"https://www.trsstaffing.com/jobs/?search={q_plus}",
        "scraped_at": today, "priority": True, "rank": 0})

    records.append({"source": "CV-Library", "region": "UK",
        "title": f"Senior / Lead {query} – CV-Library", "company": "Multiple Employers",
        "location": "UK", "salary": "N/A",
        "url": f"https://www.cv-library.co.uk/search-jobs?q={q_plus}&geo=United+Kingdom&exp=10",
        "scraped_at": today, "priority": False, "rank": 0})

    records.append({"source": "EngineeringJobs UK", "region": "UK",
        "title": f"Lead {query} – EngineeringJobs", "company": "Multiple Employers",
        "location": "UK", "salary": "N/A",
        "url": f"https://www.engineeringjobs.co.uk/jobs/{q_dash}",
        "scraped_at": today, "priority": False, "rank": 0})

    # ── Priority EPC company direct portals ──────────────────────────────────
    epc_portals = [
        ("Worley", "Worley", "India / Global", "India",
         "https://careers.worley.com/search/?q=piping+engineer&sortColumn=referencedate&sortDirection=desc"),
        ("KBR", "KBR", "Global", "UK",
         "https://careers.kbr.com/us/en/search-results?keywords=piping+engineer"),
        ("McDermott", "McDermott International", "Global", "UAE",
         "https://www.mcdermott.com/Careers"),
        ("Wood PLC", "Wood PLC", "Global", "UK",
         "https://careers.woodplc.com/jobs?q=piping"),
        ("Saipem", "Saipem", "Global", "UAE",
         "https://www.saipem.com/en/work-with-us/search-jobs?q=piping"),
        ("Jacobs", "Jacobs", "Global", "UK",
         "https://careers.jacobs.com/en_US/careers/SearchJobs/piping"),
        ("AECOM", "AECOM", "Global", "UK",
         "https://aecom.jobs/search/?q=piping+engineer"),
        ("Bechtel", "Bechtel", "Global", "UAE",
         "https://www.bechtel.com/careers/"),
        ("Penspen", "Penspen", "Global", "UK",
         "https://www.penspen.com/careers/"),
        ("AtkinsRéalis", "AtkinsRéalis", "Global", "UK",
         "https://careers.atkinsrealis.com/search/?q=piping"),
        ("GAIL", "GAIL India", "India", "India",
         "https://www.gailonline.com/final_html/EmploymentOpportunities.html"),
        ("ONGC", "ONGC", "India", "India",
         "https://ongcindia.com/wps/wcm/connect/en/career/"),
        ("Petronet LNG", "Petronet LNG", "India", "India",
         "https://www.petronetlng.com/career.aspx"),
        ("Technip Energies", "Technip Energies India", "India", "India",
         "https://jobs.technipenergies.com/go/Engineering/3868900/"),
        ("TCE Careers", "Tata Consulting Engineers", "India", "India",
         "https://www.tce.co.in/careers"),
        ("L&T Hydrocarbon", "L&T Hydrocarbon Engineering", "India", "India",
         "https://www.lntecc.com/careers/"),
        ("EIL (PSU)", "Engineers India Limited", "India", "India",
         "https://www.engineersindia.com/career"),
        ("IOCL (PSU)", "Indian Oil Corporation", "India", "India",
         "https://iocl.com/careers"),
    ]

    for source, company, location, region, portal_url in epc_portals:
        records.append({
            "source":     source,
            "region":     region,
            "title":      f"Lead / Senior {query} – Direct Portal",
            "company":    company,
            "location":   location,
            "salary":     "N/A",
            "url":        portal_url,
            "scraped_at": today,
            "priority":   True,
            "rank":       0,
        })

    filtered = [r for r in records if TITLE_KEYWORDS.search(r["title"])]
    print(f"  [India+Sites] {len(filtered)} qualifying roles added.")
    return filtered


# ─────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────
async def main(query: str = "Senior Piping Engineer") -> pd.DataFrame:
    adzuna_id  = st.secrets.get("adzuna", {}).get("app_id", "")
    adzuna_key = st.secrets.get("adzuna", {}).get("app_key", "")
    jooble_key = st.secrets.get("jooble", {}).get("api_key", "")

    all_results: list[dict] = []

    async with httpx.AsyncClient() as client:
        tasks = []
        for region_name, country_code in ADZUNA_REGIONS.items():
            tasks.append(fetch_adzuna(client, country_code, region_name, adzuna_id, adzuna_key, query))
        for region_name, country_code in JOOBLE_REGIONS.items():
            tasks.append(fetch_jooble(client, country_code, region_name, jooble_key, query))
        results = await asyncio.gather(*tasks)

    for batch in results:
        all_results.extend(batch)

    all_results.extend(fetch_indian_sources(query))
    all_results.extend(fetch_indeed(query))

    df = pd.DataFrame(all_results)
    if df.empty:
        return df

    # Ensure rank and priority columns always exist
    if "rank" not in df.columns:
        df["rank"] = 2
    if "priority" not in df.columns:
        df["priority"] = False

    df.drop_duplicates(inplace=True)
    df.drop_duplicates(subset=["title", "company", "location"], keep="first", inplace=True)
    for col in ["title", "company", "location", "salary", "url"]:
        df[col] = df[col].astype(str).str.strip()
    df["title"] = df["title"].str.title()
    df.replace(r"^\s*$", "N/A", regex=True, inplace=True)
    df = df[["source", "region", "title", "company", "location", "salary", "url", "scraped_at", "rank", "priority"]]
    df.reset_index(drop=True, inplace=True)
    return df
