"""
auto_import.py — Scrape LinkedIn/Google finance jobs and upsert new ones into Supabase.

Deduplication strategy:
  1. By job_url (link) — catches exact same posting
  2. By (title, company_name) — catches same job posted on both HH.uz and LinkedIn

Usage:
    python auto_import.py

Requires:
    pip install supabase python-dotenv

Environment variables (set in .env):
    SUPABASE_URL
    SUPABASE_SERVICE_KEY   ← service_role key from Supabase Settings → API
"""

import os
import sys
import csv
import pandas as pd
from dotenv import load_dotenv
from jobspy import scrape_jobs
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

TITLE_BLOCKLIST = [
    "smm", "продаж", "sotuv", "сотув", "маркетинг", "marketing",
    "дизайн", "dizayn", "кладовщ", "грузчик", "kassir", "кассир",
    "закупщик", "engineer", "инженер", "program manager", "customer support",
    "kitchen", "sales", "system analyst", "технической", "product owner",
    "product lead", "python", "mijozlar", "legal", "yurist", "маркетолог",
    " it", "it-", "developer", "crm", "контент-аналитик", "ai",
]


def is_finance_title(title: str) -> bool:
    lower = title.lower()
    return not any(kw in lower for kw in TITLE_BLOCKLIST)


KEYWORDS = [
    "Finance", "Accountant", "Financial Analyst",
    "Финансы", "Бухгалтер", "Аудитор", "Экономист",
    "Банк", "Кредит", "Кассир", "Аналитик",
    "Internal Audit", "Tax", "Налоги"
]

JOB_TYPE_MAP = {
    "fulltime": "full-time",
    "full-time": "full-time",
    "full_time": "full-time",
    "parttime": "part-time",
    "part-time": "part-time",
    "part_time": "part-time",
    "internship": "internship",
    "intern": "internship",
    "contract": "contract",
    "contractor": "contract",
}


def scrape_linkedin_jobs() -> list[dict]:
    print("\n--- SCRAPING LINKEDIN/GOOGLE ---")
    all_frames = []

    for term in KEYWORDS:
        print(f"  > {term}...")
        try:
            jobs = scrape_jobs(
                site_name=["linkedin", "google"],
                search_term=term,
                location="Tashkent, Uzbekistan",
                results_wanted=100,
                hours_old=168,
            )
            if jobs is not None and len(jobs) > 0:
                all_frames.append(jobs)
        except Exception as e:
            print(f"  [Error] {term}: {e}")

    if not all_frames:
        return []

    combined = pd.concat(all_frames, ignore_index=True).drop_duplicates(subset=["job_url"])
    print(f"  Scraped {len(combined)} unique jobs")
    return combined.to_dict(orient="records")


def build_salary_text(row: dict) -> str | None:
    min_amt = row.get("min_amount")
    max_amt = row.get("max_amount")
    currency = row.get("currency") or ""
    interval = row.get("interval") or ""

    if not min_amt and not max_amt:
        return None

    interval_label = f"/{interval}" if interval else ""

    if min_amt and max_amt:
        return f"{int(min_amt)} - {int(max_amt)} {currency}{interval_label}".strip()
    elif min_amt:
        return f"From {int(min_amt)} {currency}{interval_label}".strip()
    else:
        return f"Up to {int(max_amt)} {currency}{interval_label}".strip()


def map_job_type(raw: str | None) -> str:
    if not raw:
        return "full-time"
    return JOB_TYPE_MAP.get(str(raw).lower().strip(), "full-time")


def map_mode(row: dict) -> str:
    wfh = str(row.get("work_from_home_type") or "").lower()
    is_remote = row.get("is_remote")

    if "hybrid" in wfh:
        return "hybrid"
    if is_remote is True or "remote" in wfh:
        return "remote"
    return "on-site"


def fetch_existing(client: Client) -> tuple[set[str], set[tuple[str, str]]]:
    """Returns (existing_links, existing_(title,company) pairs) from DB."""
    existing_links: set[str] = set()
    existing_title_company: set[tuple[str, str]] = set()
    page_size = 1000
    offset = 0

    while True:
        result = (
            client.table("jobs")
            .select("link, title, company_name")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = result.data or []
        for row in rows:
            if row.get("link"):
                existing_links.add(row["link"])
            if row.get("title") and row.get("company_name"):
                existing_title_company.add((
                    row["title"].lower().strip(),
                    row["company_name"].lower().strip(),
                ))
        if len(rows) < page_size:
            break
        offset += page_size

    return existing_links, existing_title_company


def insert_jobs(client: Client, jobs: list[dict]) -> int:
    batch_size = 100
    inserted = 0
    for i in range(0, len(jobs), batch_size):
        batch = jobs[i : i + batch_size]
        result = client.table("jobs").insert(batch).execute()
        inserted += len(result.data or [])
    return inserted


def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print(
            "ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY in .env\n"
            "Use the service_role key (Supabase dashboard → Settings → API)."
        )
        sys.exit(1)

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    scraped = scrape_linkedin_jobs()
    if not scraped:
        print("No jobs scraped. Exiting.")
        return

    print("\n--- DEDUPLICATION ---")
    existing_links, existing_title_company = fetch_existing(client)
    print(f"  {len(existing_links)} existing jobs in database")

    to_insert = []
    skipped_url = 0
    skipped_title = 0
    skipped_invalid = 0

    for j in scraped:
        title = (j.get("title") or "").strip()
        company = (j.get("company") or "").strip()
        location = (j.get("location") or "").strip()
        link = (j.get("job_url") or "").strip()

        if not title or not company or not location:
            skipped_invalid += 1
            continue

        if not is_finance_title(title):
            skipped_invalid += 1
            continue

        if link and link in existing_links:
            skipped_url += 1
            continue

        if (title.lower(), company.lower()) in existing_title_company:
            skipped_title += 1
            continue

        salary_text = build_salary_text(j)
        min_amt = j.get("min_amount")
        max_amt = j.get("max_amount")

        to_insert.append({
            "title": title,
            "company_name": company,
            "location": location,
            "link": link or None,
            "description": (j.get("description") or None),
            "salary": salary_text,
            "salary_min": int(min_amt) if min_amt and str(min_amt) != "nan" else None,
            "salary_max": int(max_amt) if max_amt and str(max_amt) != "nan" else None,
            "type": map_job_type(j.get("job_type")),
            "mode": map_mode(j),
            "is_approved": True,
            "featured": False,
        })

    print(f"  {skipped_url} skipped (duplicate URL)")
    print(f"  {skipped_title} skipped (same title+company already in DB)")
    print(f"  {skipped_invalid} skipped (missing required fields)")
    print(f"  {len(to_insert)} new jobs to insert")

    if not to_insert:
        print("Nothing new to insert.")
        return

    print(f"\n--- INSERTING {len(to_insert)} JOBS ---")
    inserted = insert_jobs(client, to_insert)
    print(f"✅ Done. Inserted {inserted} jobs.")


if __name__ == "__main__":
    main()
