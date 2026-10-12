# Changelog

## 2026-10-12: source expansion and repairs

Sources
- Employer boards: 127 to 228. The additions cover robotics and automation (Dematic, Torc Robotics Montreal, Profound Medical, Brock Solutions), aerospace and space (Mannarino, Spaceium, NAV CANADA, Hitachi Energy, Aviya), semiconductors and hardware (Altera, Semtech, Analog Devices, Syntronic, Evertz), energy and rail (IESO, Hydro Ottawa, Toronto Hydro, VIA Rail) and university staff research jobs (University of Toronto, Waterloo, McGill, uOttawa).
- Repaired 37 boards that returned nothing or a fraction of their postings, among them BRP, CNL, Genetec, Siemens, L3Harris, Qualcomm, Kinova, CMC Electronics and Bell Textron. New parsers: Dayforce, Phenom, Taleo (REST and Business Edition), Workable widget, Jibe, paged Eightfold, SuccessFactors sitemap and multi-page search, Astro, HiBob, generic HTML rows and job links.
- Workday boards now read every Canadian posting through the board's own country or location filter; before, each search term returned the 60 most relevant postings worldwide.
- SuccessFactors boards moved from `/services/` feeds (disallowed in their robots.txt) to the search pages and sitemaps they allow. Every endpoint added or repaired in this round was checked against the site's robots.txt.
- New boards: EngineeringCareers.ca (Engineering Institute of Canada) and Espresso-Jobs (Quebec). Adzuna and Google Jobs (through SerpApi) run when API keys are supplied.
- Job Bank: the keyword feeds had gone empty; one NOC-coded feed replaces them.
- LinkedIn, Eluta and the Communitech board keep their previous query volume.

Pipeline
- Per-host request limits and a retry on HTTP 429.
- The same requisition found through several sources is merged into one row, with the employer's own posting as the main link.
- Every row gets a preliminary score from the rubric (role family, level, timing, employer, tools, French); only rows scoring 40 or more are written and remembered, which keeps the weekly state small.
- New `verify_pages.py` opens the rows scoring 45 or more and checks the hard requirements.
- Watch pages report whether their openings changed since the last run.
- Location fixes: US towns that share an Ontario city name, "Remote - US", more Ontario and Quebec towns, French seniority and program wording.

## 2026-09-22: state format and checksums
- Hashed seen keys in a line format; the loader tolerates Markdown escapes.
- SHA256SUMS written in text mode; LF line endings.

## 2026-09-21: first version
- 127 employer boards, LinkedIn guest search, Eluta, Job Bank, Vector Talent Hub, Communitech, Hacker News, ROS Discourse and eight GitHub new-grad lists.
