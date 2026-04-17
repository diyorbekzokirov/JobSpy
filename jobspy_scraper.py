import csv
import pandas as pd
from jobspy import scrape_jobs

KEYWORDS = [
    "Finance", "Accountant", "Financial Analyst", 
    "Финансы", "Бухгалтер", "Аудитор", "Экономист",
    "Банк", "Кредит", "Аналитик",
    "Internal Audit", "Tax", "Налоги"
]

all_jobs = []

for term in KEYWORDS:
    print(f"Searching: {term}")
    jobs = scrape_jobs(
        site_name=["linkedin", "google"],
        search_term=term,
        location="Tashkent, Uzbekistan",
        results_wanted=100,
        hours_old=168,
        # country_indeed='Uzbekistan',
    )
    all_jobs.append(jobs)

combined = pd.concat(all_jobs, ignore_index=True).drop_duplicates(subset=["job_url"])

print(f"Found {len(combined)} jobs")
print(combined.head())
combined.to_csv("jobs.csv", quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)
