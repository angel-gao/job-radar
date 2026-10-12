# Changelog

## 2026-10-12 (night): role type

- Every row gets a `role_type`: engineering, mixed (engineering with substantial code on real hardware, such as robotics or embedded software, firmware and flight software) or software (mostly writing code). Software rows lose 10 points so engineering roles rank first; the routine shows the type as a column in the digest.
- `verified_summary.txt` has a type column after the suggestion.

## 2026-10-12 (second pass): 37 more boards, link and scoring fixes

Sources
- Employer boards: 228 to 265, each checked against its robots.txt and tested with the fetcher. Robotics and industry: Mecka (Richmond Hill), CM Labs, Caterpillar (Laval autonomy), Celestica, Medtronic, Martinrea, OPTEL, Wabtec, Sotera Health. Utilities and rail: Enbridge, CN, Énergir. Consultancies with rail, signalling and electrical roles: GHD, Arcadis, Parsons, Mott MacDonald, Black & Veatch, Worley, Tetra Tech. AI and chips: Palona AI, Altis Labs, Google DeepMind, LG AI Research, Wind River, Infineon. Research hospitals and institutes: Sunnybrook Research Institute, SickKids, Holland Bloorview, SNOLAB, Perimeter Institute. University staff boards: McMaster, Western, York, Guelph, Ontario Tech, ÉTS and Université de Sherbrooke.
- New parsers: PeopleSoft Candidate Gateway (Fluid and classic), Technomedia, Cornerstone, Talcura, Attrax sitemap and SelectMinds. `html_rows` reads several pages and percent-encoded HTML.
- Left out: boards whose robots.txt disallows the job pages (Concordia, Queen's, Sanmina, Premier Tech search), bot challenges (Jacobs, CPKC, ISA, Ross Video's own board) and boards whose terms forbid automated collection (CareersInRobotics, AIAA's career centre).

Fixes
- Workday boards on myworkdaysite.com (Magna, Microchip) had job links that answer HTTP 500; links now use `/recruiting/<tenant>/<site>/`.
- Workday rows without a location field take it from the first bullet field or from the place in the job link. Before, they got the board's default cities, so US postings at Thomson Reuters could pass as Toronto.
- `skip_title_re` works on every board.
- `verify_pages.py` retries an unreadable page once in a cookie session and follows PeopleSoft portal frames.
- Scoring: apprentices, inspectors, ship engineer ranks, engineering technicians, quality control, and sales or recruiting titles are dropped even when the title mentions robots. French civil titles (bridges, stormwater, roads) count as off-field. "construction" no longer matches inside "Reconstruction", and "apprentissage" (machine learning) no longer counts as a new-grad word.

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
