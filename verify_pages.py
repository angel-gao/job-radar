#!/usr/bin/env python3
"""Open each candidate posting and check the hard gates of the Job Radar rubric.

Usage: python verify_pages.py candidates.json verified.json [--min 45] [--workers 6] [--max-rows 350]

For every row with prelim >= --min it fetches the posting text through the most reliable
public route (ATS JSON APIs, the LinkedIn guest fragment, plain HTML otherwise), then
records: readable or not, gate hits (years, degree, P.Eng, citizenship/clearance, closed,
deadline passed), French requirement, entry-level wording, timing wording and a verbatim
salary string. Standard library only; uses job_radar_fetch.http (per-host throttling).
"""
import json
import re
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import job_radar_fetch as jr

LI_LOCK = threading.Lock()
LI_LAST = [0.0]


def _li_wait(pace=4.0):
    with LI_LOCK:
        w = LI_LAST[0] + pace - time.time()
        if w > 0:
            time.sleep(w)
        LI_LAST[0] = time.time()


def text_of(html_text, limit=60000):
    t = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", html_text or "")
    t = re.sub(r"(?i)<br\s*/?>|</p>|</li>|</div>|</h\d>", "\n", t)
    return jr.strip_tags(t, limit)


def _workday_api_text(api):
    j, err = jr.get_json(api, headers={"Accept": "application/json"})
    if not j:
        return None
    info = j.get("jobPostingInfo") or {}
    t = " ".join(str(info.get(k, "")) for k in ("title", "location", "additionalLocations", "timeType", "startDate", "postedOn"))
    return t + "\n" + text_of(info.get("jobDescription", ""))


def fetch_posting(url, row=None):
    """Return (route, text) or (route, None)."""
    row = row or {}
    if row.get("detail_api") and url == row.get("url"):
        t = _workday_api_text(row["detail_api"])
        if t:
            return "workday_cxs", t
    u = url or ""
    p = urllib.parse.urlsplit(u)
    host = p.netloc.lower()
    try:
        m = re.search(r"linkedin\.com/jobs/view/(?:[^/?]*-)?(\d{6,})", u)
        if m:
            _li_wait()
            st, t = jr.http("https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/%s" % m.group(1), retry_429=False)
            if st == 429:
                time.sleep(60)
                _li_wait()
                st, t = jr.http("https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/%s" % m.group(1), retry_429=False)
            if st and st < 400 and len(t) > 500:
                return "linkedin_guest", text_of(t)
            return "linkedin_guest", None
        if "myworkdaysite.com" in host:
            # https://wd3.myworkdaysite.com/<Site>/job/<rest> (Magna): the tenant is the site name in lower case
            parts = [x for x in p.path.split("/") if x]
            if "job" in parts:
                i = parts.index("job")
                site = parts[i - 1]
                t = _workday_api_text("https://%s/wday/cxs/%s/%s/%s" % (host, site.lower(), site, "/".join(parts[i:])))
                if t:
                    return "workday_cxs", t
        if "myworkdayjobs.com" in host:
            parts = [x for x in p.path.split("/") if x]
            # /<lang?>/<site>/job/<rest...>  ->  /wday/cxs/<tenant>/<site>/job/<rest...>
            if "job" in parts:
                i = parts.index("job")
                site = parts[i - 1]
                tenant = host.split(".")[0]
                api = "https://%s/wday/cxs/%s/%s/%s" % (host, tenant, site, "/".join(parts[i:]))
                j, err = jr.get_json(api, headers={"Accept": "application/json"})
                if j:
                    info = j.get("jobPostingInfo") or {}
                    t = " ".join(str(info.get(k, "")) for k in ("title", "location", "additionalLocations", "timeType", "startDate", "postedOn"))
                    return "workday_cxs", t + "\n" + text_of(info.get("jobDescription", ""))
        if "oraclecloud.com" in host and "/job/" in p.path:
            jid = re.search(r"/job/(\d+)", p.path).group(1)
            sm = re.search(r"/sites/([^/]+)/", p.path)
            site = sm.group(1) if sm else "CX_1"
            api = ("https://%s/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails?expand=all&onlyData=true"
                   "&finder=ById;Id=%s,siteNumber=%s" % (host, jid, site))
            j, err = jr.get_json(api, headers={"Accept": "application/json"})
            if j and j.get("items"):
                it = j["items"][0]
                keys = ("Title", "PrimaryLocation", "ExternalPostedStartDate", "ExternalPostedEndDate", "JobFunction",
                        "ExternalDescriptionStr", "ExternalQualificationsStr", "ExternalResponsibilitiesStr", "CorporateDescriptionStr")
                return "oracle_rest", "\n".join(text_of(str(it.get(k) or "")) for k in keys)
        m = re.search(r"(?:boards|job-boards)\.greenhouse\.io/([^/]+)/jobs/(\d+)", u) or re.search(r"[?&]gh_jid=(\d+)", u)
        if m and m.lastindex == 2:
            j, err = jr.get_json("https://boards-api.greenhouse.io/v1/boards/%s/jobs/%s" % (m.group(1), m.group(2)))
            if j:
                return "greenhouse_api", (j.get("title", "") + " " + str((j.get("location") or {}).get("name", "")) + "\n"
                                          + text_of(jr.html.unescape(j.get("content", ""))))
        m = re.search(r"jobs\.lever\.co/([^/]+)/([0-9a-f-]{36})", u)
        if m:
            j, err = jr.get_json("https://api.lever.co/v0/postings/%s/%s" % (m.group(1), m.group(2)))
            if j:
                lists = " ".join(x.get("text", "") + " " + text_of(x.get("content", "")) for x in j.get("lists", []) or [])
                return "lever_api", "%s %s\n%s\n%s\n%s" % (j.get("text", ""), (j.get("categories") or {}).get("location", ""),
                                                          j.get("descriptionPlain", ""), lists, j.get("additionalPlain", ""))
        m = re.search(r"jobs\.ashbyhq\.com/([^/]+)/([0-9a-f-]{36})", u)
        if m:
            j, err = jr.get_json("https://api.ashbyhq.com/posting-api/job-board/%s?includeCompensation=true" % m.group(1))
            if j:
                for job in j.get("jobs", []):
                    if m.group(2) in (job.get("jobUrl") or "") or job.get("id") == m.group(2):
                        comp = (job.get("compensation") or {}).get("compensationTierSummary") or ""
                        return "ashby_api", "%s %s %s\n%s" % (job.get("title", ""), job.get("location", ""), comp,
                                                             text_of(job.get("descriptionHtml", "")))
        m = re.search(r"jobs\.smartrecruiters\.com/([^/]+)/(\d+)", u)
        if m:
            j, err = jr.get_json("https://api.smartrecruiters.com/v1/companies/%s/postings/%s" % (m.group(1), m.group(2)))
            if j:
                secs = (j.get("jobAd") or {}).get("sections") or {}
                return "smartrecruiters_api", (j.get("name", "") + "\n" + "\n".join(text_of((v or {}).get("text", "")) for v in secs.values()))
        m = re.search(r"apply\.workable\.com/([^/]+)/j/([0-9A-Za-z]+)", u)
        if m:
            j, err = jr.get_json("https://apply.workable.com/api/v2/accounts/%s/jobs/%s" % (m.group(1), m.group(2)))
            if j:
                return "workable_api", "%s %s\n%s\n%s\n%s" % (j.get("title", ""), json.dumps(j.get("location", {})),
                                                             text_of(j.get("description", "")), text_of(j.get("requirements", "")),
                                                             text_of(j.get("benefits", "")))
        m = re.search(r"([a-z0-9-]+)\.bamboohr\.com/careers/(\d+)", u)
        if m:
            j, err = jr.get_json("https://%s.bamboohr.com/careers/%s/detail" % (m.group(1), m.group(2)))
            if j:
                jo = (j.get("result") or {}).get("jobOpening") or {}
                return "bamboohr_api", "%s %s\n%s" % (jo.get("jobOpeningName", ""), json.dumps(jo.get("location", {})),
                                                     text_of(jo.get("description", "")))
        st, t = jr.http(u, timeout=30)
        if st and st < 400:
            tx = text_of(t)
            # JSON-LD JobPosting often carries the full description even on JS-heavy pages
            for blob in re.findall(r'(?is)<script[^>]+application/ld\+json[^>]*>(.*?)</script>', t):
                if "JobPosting" in blob:
                    try:
                        d = json.loads(blob.strip())
                        d = d[0] if isinstance(d, list) else d
                        tx = "%s %s\n%s\n%s" % (d.get("title", ""), json.dumps(d.get("jobLocation", ""))[:300],
                                                text_of(d.get("description", "")), tx)
                    except Exception:
                        pass
            return "html", tx if len(tx) > 400 else None
        return "html", None
    except Exception as e:
        return "error:%s" % type(e).__name__, None


WORD_NUM = {"one": "1", "two": "2", "three": "3", "four": "4", "five": "5", "six": "6", "seven": "7", "eight": "8",
            "nine": "9", "ten": "10", "un": "1", "une": "1", "deux": "2", "trois": "3", "quatre": "4", "cinq": "5"}
WORD_NUM_RE = re.compile(r"(?i)\b(one|two|three|four|five|six|seven|eight|nine|ten|deux|trois|quatre|cinq)\b(?=\s*(?:\(\d+\)\s*)?\+?\s*(?:years?|yrs|ans|ann[ée]es))")
YEARS_RE = re.compile(r"(?i)(?:minimum|at least|min\.?|over|plus de|au moins)?\s*\(?(\d{1,2})\)?\s*(?:\+|-\s*\d{1,2}|to \d{1,2}|à \d{1,2}|or more)?\s*"
                      r"(?:\+\s*)?(?:years?|yrs|ans|année)s?\b[^.\n]{0,60}?(?:experience|exp[ée]rience|of (?:industry|professional|relevant|hands-on|work))")
RANGE_LOW_RE = re.compile(r"(?i)\b(\d{1,2})\s*(?:-|–|to|à)\s*(\d{1,2})\s*(?:years?|yrs|ans)")
DEGREE_REQ_RE = re.compile(r"(?i)(master'?s|m\.?sc|m\.?eng|ph\.?\s?d|doctora\w+|ma[îi]trise)[^.\n]{0,80}?(required|is required|requis|exig|must have|mandatory)"
                           r"|(required|requis|must have)[^.\n]{0,40}?(master'?s|ph\.?\s?d|doctora\w+)"
                           r"|(?:minimum|required) qualifications?:?[^.\n]{0,40}?(master'?s|ph\.?\s?d)"
                           r"|(?:you bring to the role|you bring|you have|requirements?:|qualifications?:)[^.\n]{0,80}?\b(?:a |an )?(?:master'?s|master’s) (?:or|and|/) ?ph\.?\s?d"
                           r"|(?:eligible|you possess|must possess|must hold|candidates must have)[^.\n]{0,60}?(?:ph\.?\s?d|master'?s|master’s)"
                           r"|(?:finishing|recently graduated from|completed or soon-to-be-completed)[^.\n]{0,60}?(?:master'?s|master’s|ph\.?\s?d) (?:or ph\.?\s?d )?(?:program|degree)")
DEGREE_STUDENT_RE = re.compile(r"(?i)(currently|actively) (enrolled|pursuing) (in )?(a )?(ph\.?\s?d|master)")
PENG_RE = re.compile(r"(?i)\bP\.?\s?Eng\.?\b[^.\n]{0,50}(required|is required|designation required|must)|licensed professional engineer[^.\n]{0,30}required|membre en r[èe]gle de l'OIQ[^.\n]{0,40}(exig|requis)")
ELIG_RE = re.compile(r"(?i)must be a canadian citizen|canadian citizenship (is )?(required|mandatory)|citizenship or permanent residen\w+ (is )?required|"
                     r"must (be|hold) (a )?(canadian )?(citizen|permanent resident)|must have (pr|permanent resident) status|"
                     r"authori[sz]ed to work in canada on a (permanent|ongoing|indefinite) basis|top secret|enhanced top secret|polygraph|"
                     r"\bITAR\b|level ii secret|secret (security )?clearance (is )?(required|a condition)|"
                     r"canadian armed forces member|citoyennet[ée] canadienne (est )?(requise|exig)")
US_PERSON_RE = re.compile(r"\bU\.S\. [Pp]erson|\bUS [Pp]ersons?\b|\bU\.S\. citizens? only")   # case-sensitive on purpose
EAR_RE = re.compile(r"(?i)export administration regulations|\bEAR\b country group|contingent upon your citizenship")
CLOSED_RE = re.compile(r"(?i)no longer accepting applications|job (is )?no longer available|posting (has )?(expired|closed)|position has been filled|"
                       r"this job (has expired|is closed)|n'est plus disponible|offre (a )?expir")
FR_REQ_RE = jr.FR_REQ_RE
ENTRY_RE = re.compile(r"(?i)new grad|new graduate|recent(ly)? grad|entry[- ]level|early[- ]career|early talent|university grad|"
                      r"job category: campus|\bEIT\b|engineer[- ]in[- ]training|\bCPI\b|nouveaux? dipl[ôo]m|finissant|"
                      r"0\s*(?:-|–|to)\s*[1-3] (?:years|ans)|graduate program|programme (de )?(rel[eè]ve|dipl[ôo]m)|"
                      r"(?<!mentor )(?<!mentoring )(?<!supervise )(?<!coach )(?<!guide )(?<!train )\bjunior (engineer|developer|ing[ée]nieur)")
NAV_ENTRY_RE = re.compile(r"(?i)programmes? pour les nouveaux dipl[ôo]m[ée]s|stages?\s*(et|&)\s*programmes?|"
                          r"recent grad(uate)?s? and student jobs|new grads?,?\s*co-?ops?,?\s*(and |& )?internships?|"
                          r"early careers?\s*(life at|benefits|hiring|login|current)|best employers for recent graduates|"
                          r"entry\s*(&|and)\s*new graduates|new grads?\s*(&|and)\s*interns?|"
                          r"new grads?\s*/\s*students?|students?\s*(/|&|and)\s*new grad(uate)?s?|internships?\s*(&|and)\s*new graduate"
                          r" programs?|new graduate programs?\s*(&|and)\s*internships?|campus\s*/\s*students?|"
                          r"[ée]tudiants?\s*(et|&|/)\s*(nouveaux )?dipl[ôo]m[ée]s|stages?\s*(et|&)\s*(nouveaux )?dipl[ôo]m[ée]s")
TIMING_RE = re.compile(r"(?i)\b2027\b|graduating|expected graduation|upon graduation|(?:may|june|summer|spring) 2027")
SALARY_RE = re.compile(r"(?i)(?:\$\s?\d[\d,]*(?:\.\d+)?\s*(?:k|K)?(?:\s*(?:CAD|USD))?\s*(?:/\s*(?:yr|year|hour|hr|an|heure))?\s*(?:-|–|to|à)\s*\$?\s?\d[\d,]*(?:\.\d+)?\s*(?:k|K)?(?:\s*(?:CAD|USD))?(?:\s*(?:/|per)\s*(?:yr|year|hour|hr|an|annum|annually|heure))?)"
                       r"|(?:\d[\d\s,]*\s?\$\s*(?:-|–|à)\s*\d[\d\s,]*\s?\$)")
INTERN_ONLY_RE = re.compile(r"(?i)\b(co-?op|internship|stage|work term|PEY)\b")


def verify_row(row):
    url = row.get("url", "")
    route, txt = fetch_posting(url, row)
    tried = [url]
    if txt is None:
        for alt in row.get("alt_urls", [])[:3]:
            route, txt = fetch_posting(alt, row)
            tried.append(alt)
            if txt:
                url = alt
                break
    if txt is None and len(row.get("detail_text") or "") > 400:
        # script-rendered page, but the board's list API gave the full description
        route, txt, url = "list_api_text", row["detail_text"], row.get("url", "")
    rec = {"key": row.get("key"), "url_opened": url if txt else None, "route": route, "readable": bool(txt)}
    if not txt:
        return rec
    words = [w for w in jr.norm_title(row.get("title", "")).split() if len(w) > 3]
    hits = sum(1 for w in words if w in jr._ascii(txt[:20000]))
    rec["title_match"] = round(hits / len(words), 2) if words else 1.0
    if words and rec["title_match"] < 0.5:
        rec["readable"] = False
        rec["why_unreadable"] = "page text does not contain the job title (landing page or script-rendered)"
        return rec
    t = txt[:60000]
    t = WORD_NUM_RE.sub(lambda m: WORD_NUM[m.group(1).lower()], t)
    gates = []
    yrs = []
    for m in YEARS_RE.finditer(t):
        before = t[max(0, m.start() - 30): m.start() + 12]
        # "less than 2 years of post-graduation experience" is an upper bound (new-grad programs), not a requirement
        if re.search(r"(?i)less than|no more than|not more than|fewer than|maximum|max\.|up to|under|moins de|au plus|jusqu", before):
            continue
        try:
            yrs.append((int(m.group(1)), m.group(0).strip()[:140]))
        except ValueError:
            pass
    low = [(int(a), int(b)) for a, b in RANGE_LOW_RE.findall(t)]
    min_req = None
    for n, phrase in yrs:
        if re.search(r"(?i)\b0\s*(-|–|to)", phrase):
            continue
        if re.search(r"(?i)prefer|asset|atout|nice to have|bonus|ideally", phrase):
            continue
        if n >= 12:   # "over 45 years of experience" describes the company, not the candidate
            continue
        min_req = n if min_req is None else max(min_req, n)
    if min_req is not None and min_req >= 2 and not any(a == 0 for a, b in low):
        gates.append("years: " + next(p for n, p in yrs if n == min_req))
    m = DEGREE_REQ_RE.search(t)
    if m and not re.search(r"(?i)bachelor'?s|b\.?eng|b\.?sc|undergraduate|baccalaur", t[max(0, m.start() - 160): m.end() + 40]):
        gates.append("degree: " + m.group(0)[:140])
    m = DEGREE_STUDENT_RE.search(t)
    if m:
        gates.append("degree_student: " + m.group(0)[:140])
    m = PENG_RE.search(t)
    if m:
        gates.append("peng: " + m.group(0)[:140])
    m = ELIG_RE.search(t)
    if m:
        ctx = t[max(0, m.start() - 200): m.end() + 120]
        if not re.search(r"(?i)equal (employment )?opportunity|eeo|affirmative action|protected veteran", ctx):
            gates.append("eligibility: " + m.group(0)[:140])
    m = US_PERSON_RE.search(t)
    if m:
        ctx = t[max(0, m.start() - 200): m.end() + 120]
        if not re.search(r"(?i)equal (employment )?opportunity|eeo|affirmative action|protected veteran", ctx):
            gates.append("eligibility: " + t[max(0, m.start() - 40): m.end() + 40].replace("\n", " "))
    m = EAR_RE.search(t)
    if m:
        gates.append("export_control: " + m.group(0)[:140])
    m = CLOSED_RE.search(t)
    if m:
        gates.append("closed: " + m.group(0)[:100])
    rec["gates"] = gates
    m = FR_REQ_RE.search(t)
    rec["french_required"] = m.group(0)[:120] if m else ""
    # site menus ("New Grads/Students", "Internships & New Graduate Programs") are not wording about this job
    t_body = NAV_ENTRY_RE.sub(" ", t)
    m = ENTRY_RE.search(t_body)
    rec["entry_wording"] = t_body[max(0, m.start() - 60): m.end() + 80].replace("\n", " ") if m else ""
    m = TIMING_RE.search(t)
    rec["timing_wording"] = t[max(0, m.start() - 80): m.end() + 60].replace("\n", " ") if m else ""
    m = SALARY_RE.search(t)
    rec["salary"] = t[max(0, m.start() - 60): m.end() + 10].replace("\n", " ").strip() if m else ""
    rec["intern_words"] = bool(INTERN_ONLY_RE.search(row.get("title", "")))
    rec["text_head"] = t[:1500]
    # the part of the page a reader checks first: requirements / qualifications (site menus fill the head)
    rm = None
    for rx in REQ_HEAD_TIERS:
        rm = rx.search(t)
        if rm:
            break
    rec["requirements_excerpt"] = t[rm.start(): rm.start() + 1800] if rm else ""
    rec["chars"] = len(t)
    rec["_text"] = t
    return rec


BACHELOR_RE = re.compile(r"(?i)bachelor'?s|b\.?\s?eng\b|b\.?\s?a\.?\s?sc|b\.?\s?sc\b|undergraduate degree|baccalaur[ée]at|degree in (engineering|"
                         r"computer|electrical|mechanical|mechatronics|science)")
COHORT_2026_RE = re.compile(r"(?i)graduat\w* (in|by|between [a-z]+ \d{4} and) [a-z]* ?2026\b|class of 2026|2026 (new )?grad(uate)?s?\b|"
                            r"new grad 2026|dipl[ôo]m[ée]s? (en|de) 2026")
REQ_HEAD_TIERS = [re.compile(p) for p in (
    r"(?i)\b(you bring to the role|what you bring|required qualifications|minimum qualifications|basic qualifications|"
    r"qualifications requises|ce que vous apportez|profil recherch\w*|exigences du poste)\b",
    r"(?i)\b(qualifications|what you.ll need|what we.re looking for|about you|who you are|must[- ]haves?|exigences)\b",
    r"(?i)\b(requirements|you have)\b")]
MID_YEARS_RE = re.compile(r"(?i)\b1\s*(?:-|–|to|à)\s*3\s*(?:years?|yrs|ans)|\b1\+?\s*(?:years?|yrs|ans)\b")


def adjust(row, rec):
    """Apply the rubric with what the page shows. Returns (adjusted score, suggestion, notes)."""
    if not rec.get("readable"):
        return row.get("prelim", 0), "unverified", [rec.get("why_unreadable") or "page not readable"]
    if rec.get("gates"):
        return 0, "drop", list(rec["gates"])
    parts = row.get("score_parts") or {}
    score = row.get("prelim", 0)
    notes = []
    level = parts.get("level", 0)
    if rec.get("entry_wording") and level < 30:
        score += 30 - level
        notes.append("entry wording on page")
    elif level < 15 and BACHELOR_RE.search(rec.get("_text", "")) and not MID_YEARS_RE.search(rec.get("_text", "")):
        score += 15 - level
        notes.append("bachelor's degree, no experience bar")
    elif level < 8 and MID_YEARS_RE.search(rec.get("_text", "")):
        score += 8 - level
        notes.append("1-3 years")
    if rec.get("timing_wording") and parts.get("timing", 0) < 15:
        score += 15 - parts.get("timing", 0)
        notes.append("2027 / graduation timing on page")
    if rec.get("salary") and parts.get("extras", 0) < 10:
        score += 3
    if rec.get("french_required") and not parts.get("french"):
        score -= 10
        notes.append("French required")
    if COHORT_2026_RE.search(rec.get("_text", "")[:20000]) and "2027" not in (rec.get("timing_wording") or ""):
        score -= 15
        notes.append("2026 graduating cohort")
    score = max(0, min(100, score))
    return score, ("keep" if score >= 50 else "below_50"), notes


def main():
    src, dst = sys.argv[1], sys.argv[2]
    mn = 45
    workers = 6
    if "--min" in sys.argv:
        mn = int(sys.argv[sys.argv.index("--min") + 1])
    if "--workers" in sys.argv:
        workers = int(sys.argv[sys.argv.index("--workers") + 1])
    cap = int(sys.argv[sys.argv.index("--max-rows") + 1]) if "--max-rows" in sys.argv else 350
    rows = [r for r in json.load(open(src, encoding="utf-8")) if r.get("prelim", 0) >= mn]
    rows.sort(key=lambda r: -r.get("prelim", 0))
    skipped = rows[cap:]
    rows = rows[:cap]
    if skipped:
        print("opening the %d highest-scoring rows; %d rows (prelim %s and below) left unopened"
              % (len(rows), len(skipped), skipped[0].get("prelim")))
    t0 = time.time()

    def work(r):
        rec = verify_row(r)
        return rec

    with ThreadPoolExecutor(max_workers=workers) as ex:
        recs = list(ex.map(work, rows))
    out = []
    for r, v in zip(rows, recs):
        adj, sug, notes = adjust(r, v)
        v.pop("_text", None)
        out.append(dict(r, verify=v, adjusted=adj, suggestion=sug, adjust_notes=notes))
    out.sort(key=lambda x: ({"keep": 0, "unverified": 1, "below_50": 2, "drop": 3}[x["suggestion"]], -x["adjusted"]))
    json.dump(out, open(dst, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    summ = dst.rsplit(".", 1)[0] + "_summary.txt"
    with open(summ, "w", encoding="utf-8") as f:
        f.write("adjusted | prelim | suggestion | title | company | location | detail | url\n")
        for x in out:
            v = x["verify"]
            if x["suggestion"] == "drop":
                detail = "; ".join(v.get("gates", []))
            elif x["suggestion"] == "unverified":
                detail = "; ".join(x["adjust_notes"])
            else:
                detail = "entry: %s | salary: %s | FR: %s | %s" % ((v.get("entry_wording") or "-")[:90], (v.get("salary") or "-")[:90],
                                                                 v.get("french_required") or "-", ", ".join(x["adjust_notes"]))
            f.write("%s | %s | %s | %s | %s | %s | %s | %s\n" % (x["adjusted"], x.get("prelim"), x["suggestion"], x.get("title", "")[:90],
                                                               (x.get("company") or "")[:40], (x.get("location") or "")[:40],
                                                               detail.replace("\n", " "), v.get("url_opened") or x.get("url")))
    from collections import Counter
    c = Counter(x["suggestion"] for x in out)
    print("verified %d rows in %.0fs: %s; summary in %s" % (len(rows), time.time() - t0, dict(c), summ))


if __name__ == "__main__":
    main()
