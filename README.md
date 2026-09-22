# job-radar fetcher

Weekly job-posting fetcher for a personal new-grad job alert (Ontario/Quebec, robotics / ML / controls / embedded).
`job_radar_fetch.py` is standard-library Python that pulls postings from public employer job boards (Greenhouse, Lever,
Ashby, Workday, SmartRecruiters, Workable, Recruitee, BambooHR, Oracle HCM, iCIMS, Eightfold, SuccessFactors and others),
public aggregators (LinkedIn guest search, Eluta RSS, Job Bank Atom, Vector Institute Talent Hub RSS, Communitech Getro API,
Hacker News Algolia, ROS Discourse) and GitHub new-grad lists, then prefilters for Ontario/Quebec entry-level engineering roles.
`sources.json` lists the boards and queries. `SHA256SUMS` lets the consumer verify a download.

Usage: `python3 job_radar_fetch.py --sources sources.json --seen seen.json --out candidates.json --stats stats.json`

No personal data is stored here. Employer endpoints are public job-board APIs; be polite (the script paces LinkedIn and Job Bank).
