# job-radar fetcher

Weekly job-posting fetcher for a personal new-grad job alert (Ontario/Quebec; robotics, ML, controls, embedded).

| File | What it does |
|---|---|
| `job_radar_fetch.py` | Standard-library Python. Pulls postings from public employer job boards (Greenhouse, Lever, Ashby, Workday, SmartRecruiters, Workable, Recruitee, BambooHR, Oracle HCM, iCIMS/Jibe, Eightfold, SuccessFactors, Phenom, Taleo, Dayforce, PeopleSoft, Cornerstone, Talcura, SelectMinds and others) and from public aggregators, prefilters for Ontario/Quebec entry-level engineering roles, merges the same requisition found through several sources, gives each row a preliminary score, and writes `candidates.json`, `stats.json` and next week's seen state. |
| `verify_pages.py` | Opens the postings that scored 45 or more, checks the hard requirements (years of experience, degree, P.Eng, citizenship or clearance, closed postings), re-scores them with what the page says, and writes `verified.json` plus a one-line-per-row summary. |
| `sources.json` | Employer boards, aggregator queries and program watch pages. |
| `SHA256SUMS` | Checksums so a consumer can verify a download. |
| `CHANGELOG.md` | What changed and when. |

Usage:

```
python3 job_radar_fetch.py --sources sources.json --seen seen.txt --seen-out seen_next.txt \
    --out candidates.json --stats stats.json [--keys keys.txt] [--raw-out raw.json]
python3 verify_pages.py candidates.json verified.json --min 45
```

The seen state is a small text file: a header line, then lines of a date followed by 8-hex keys (one key per posting written, keyed by company, title and city so a posting found again through another source is still recognised). Older formats are read too.

Optional sources that need a personal API key (Adzuna, Google Jobs through SerpApi) are skipped when `--keys` is absent. No keys or personal data are stored here.

Politeness: requests are limited per host (and per Workday data centre), LinkedIn and Job Bank are paced, and every endpoint added after September 2026 was checked against the site's robots.txt.
