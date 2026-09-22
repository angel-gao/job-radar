#!/usr/bin/env python3
"""Job Radar fetcher.

Pulls postings from employer job boards (many ATS types), public aggregators
(LinkedIn guest API, Eluta RSS, Job Bank Atom, Vector Talent Hub RSS,
Communitech Getro API, Hacker News Algolia, ROS Discourse), GitHub new-grad
lists, and a set of program "watch pages". Prefilters for Ontario/Quebec,
non-senior, engineering or entry-level postings that are not in the seen set,
then writes candidates.json and stats.json for the scoring step.

Standard library only. Python 3.8+.

Usage:
  python job_radar_fetch.py --sources sources.json --seen seen.json \
      --out candidates.json --stats stats.json [--skip linkedin,jobbank] \
      [--quick] [--workers 8] [--max-per-source 300] [--only "Waabi,Cohere"]
"""
import argparse
import datetime as dt
import email.utils
import gzip
import hashlib
import html
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
TODAY = dt.date.today()

# ----------------------------------------------------------------------------
# HTTP
# ----------------------------------------------------------------------------

def http(url, method="GET", body=None, headers=None, timeout=25):
    """Return (status, text). status is None on transport failure (text = error)."""
    hdrs = {"User-Agent": UA, "Accept": "*/*",
            "Accept-Language": "en-CA,en;q=0.9,fr;q=0.8",
            "Accept-Encoding": "gzip"}
    if headers:
        hdrs.update(headers)
    data = None
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/json")
        else:
            data = str(body).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/x-www-form-urlencoded")
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            status = r.status
            enc = r.headers.get("Content-Encoding", "")
    except urllib.error.HTTPError as e:
        try:
            raw = e.read()
        except Exception:
            raw = b""
        status = e.code
        enc = e.headers.get("Content-Encoding", "") if e.headers else ""
    except Exception as e:  # timeout, DNS, TLS...
        if "SSL" in str(e) or "handshake" in str(e).lower():
            return _curl(url, method, data, hdrs, timeout)
        return None, "%s: %s" % (type(e).__name__, e)
    if enc and "gzip" in enc:
        try:
            raw = gzip.decompress(raw)
        except Exception:
            pass
    return status, raw.decode("utf-8", errors="replace")


def _curl(url, method, data, hdrs, timeout):
    """Fallback fetch through the curl binary (different TLS stack)."""
    import subprocess
    cmd = ["curl", "-sL", "--compressed", "--max-time", str(timeout), "-X", method, "-w", "\n%{http_code}"]
    for k, v in hdrs.items():
        if k.lower() != "accept-encoding":
            cmd += ["-H", "%s: %s" % (k, v)]
    if data is not None:
        cmd += ["--data-binary", data.decode("utf-8", errors="replace")]
    cmd.append(url)
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout + 10)
    except Exception as e:
        return None, "curl: %s" % e
    out = r.stdout.decode("utf-8", errors="replace")
    if r.returncode != 0 or "\n" not in out:
        return None, "curl exit %s" % r.returncode
    body, _, code = out.rpartition("\n")
    try:
        return int(code.strip()), body
    except ValueError:
        return None, "curl: no status"


def get_json(url, method="GET", body=None, headers=None):
    st, txt = http(url, method, body, headers)
    if st is None:
        return None, txt
    if st >= 400:
        return None, "HTTP %s" % st
    try:
        return json.loads(txt), None
    except Exception:
        # some APIs prefix anti-JSON-hijack tokens
        m = re.search(r"[\[{]", txt)
        if m:
            try:
                return json.loads(txt[m.start():]), None
            except Exception:
                pass
        return None, "not JSON"


# ----------------------------------------------------------------------------
# Text helpers
# ----------------------------------------------------------------------------

TAG_RE = re.compile(r"<[^>]+>")


def strip_tags(s, limit=None):
    if not s:
        return ""
    s = html.unescape(TAG_RE.sub(" ", str(s)))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:limit] if limit else s


def norm_date(v):
    """Best-effort ISO date (YYYY-MM-DD) from many formats; None if unknown."""
    if v is None or v == "":
        return None
    try:
        if isinstance(v, (int, float)):
            x = float(v)
            if x > 1e11:
                x = x / 1000.0
            return dt.datetime.utcfromtimestamp(x).date().isoformat()
    except Exception:
        return None
    s = str(v).strip()
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    m = re.search(r"posted\s+(today|yesterday|(\d+)\+?\s+days?\s+ago|(\d+)\+?\s+hours?\s+ago)", s, re.I)
    if m:
        if m.group(1).lower() == "today" or m.group(3):
            return TODAY.isoformat()
        if m.group(1).lower() == "yesterday":
            return (TODAY - dt.timedelta(days=1)).isoformat()
        return (TODAY - dt.timedelta(days=int(m.group(2)))).isoformat()
    if re.match(r"^\d{9,13}$", s):
        return norm_date(int(s))
    try:
        d = email.utils.parsedate_to_datetime(s)
        if d:
            return d.date().isoformat()
    except Exception:
        pass
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d", "%b %d %Y"):
        try:
            return dt.datetime.strptime(s[:20].strip(), fmt).date().isoformat()
        except Exception:
            continue
    return None


def abs_url(base, href):
    if not href:
        return ""
    href = html.unescape(href.strip())
    return urllib.parse.urljoin(base, href)


def canon_url(u):
    if not u:
        return ""
    u = u.strip()
    p = urllib.parse.urlsplit(u)
    q = [(k, v) for k, v in urllib.parse.parse_qsl(p.query) if not k.lower().startswith(("utm_", "ref", "src", "trk", "source"))]
    return urllib.parse.urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path.rstrip("/"), urllib.parse.urlencode(q), ""))


def item(title, url, location="", posted=None, snippet="", company="", source="", extra=None):
    d = {"title": strip_tags(title, 200), "url": url or "", "location": strip_tags(location, 120),
         "posted": norm_date(posted) if not (isinstance(posted, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", posted)) else posted,
         "snippet": strip_tags(snippet, 400), "company": strip_tags(company, 120), "source": source}
    if extra:
        d.update(extra)
    return d


# ----------------------------------------------------------------------------
# Filters
# ----------------------------------------------------------------------------

LOC_RE = re.compile(
    r"(Ontario|, ?ON\b|\bON,|\bON$|Toronto|Ottawa|Kanata|Nepean|Waterloo|Kitchener|Cambridge|Guelph|Mississauga|Markham|Oakville|"
    r"Brampton|Vaughan|Concord|Richmond Hill|Burlington|Hamilton|Waterdown|London,?\s*(ON|Ontario)|Windsor|Oshawa|"
    r"Pickering|Peterborough|Kingston|Sudbury|Lively|Niagara|Alliston|Aurora|Newmarket|Milton|Chalk River|Tiverton|"
    r"Whitby|Bowmanville|Clarington|Collingwood|North Bay|Arnprior|Woodbridge|Scarborough|Etobicoke|North York|Ajax|"
    r"Haley Station|Fort Erie|Port Hope|Barrie|Owen Sound|Woodstock|Norwich|Quebec|Qu[ée]bec|, ?QC\b|\bQC,|\bQC$|Montr[ée]al|Laval|"
    r"Longueuil|Boucherville|Brossard|Mirabel|Saint-Laurent|St-Laurent|St\.? Laurent|Dorval|Kirkland|Pointe-Claire|"
    r"Sherbrooke|Gatineau|L[ée]vis|Boisbriand|Blainville|Varennes|Valcourt|Saint-Bruno|Lachine|Sainte-Anne-de-Bellevue|"
    r"Saint-Hubert|Bromont|Trois-Rivi[èe]res|Saguenay|Saint-Eustache|Sainte-Claire|Mont-Royal|Mount-Royal|Gentilly)",
    re.I)
OUTSIDE_RE = re.compile(
    r"\b(BC|British Columbia|Vancouver|Victoria|Burnaby|Richmond, BC|Alberta|Calgary|Edmonton|Manitoba|Winnipeg|"
    r"Saskatchewan|Saskatoon|Regina|Nova Scotia|Halifax|Dartmouth|New Brunswick|Fredericton|Moncton|Newfoundland|"
    r"St\.? John'?s|PEI|Prince Edward|Yukon|Nunavut|Northwest Territories|United States|USA|U\.S\.|California|Texas|"
    r"Washington|New York|Massachusetts|Michigan|Arizona|Colorado|Florida|Georgia|Illinois|Pennsylvania|Virginia|"
    r"North Carolina|Ohio|Oregon|Nevada|Utah|Minnesota|Wisconsin|Maryland|Bethesda|India|Bangalore|Bengaluru|Hyderabad|"
    r"Pune|Chennai|Germany|N[uü]rnberg|France|Paris|United Kingdom|England|Israel|Poland|Sweden|Finland|Japan|Korea|"
    r"Seoul|China|Shanghai|Beijing|Shenzhen|Singapore|Australia|Mexico|Brazil|Netherlands|Ireland|Dublin|Spain|Italy|"
    r"Switzerland|Belgium|Romania|Czech|Hungary|Portugal|Taiwan|Vietnam|Philippines|Malaysia)\b|\bUS-[A-Z]{2}\b|^(GB|US|DE|FR|IN|MX|AU|PL|SE|CZ)\.|, ?(US|USA|MX|AU|GB|UK|DE|FR|IN|CN|JP|KR|SG|BR|IE|NL|PL|IL|SE|FI|ES|IT|CH|BE|RO|CZ|HU|PT|TW|VN|PH|MY|AE|SA)\b|\b[A-Z]{2}, (US|USA)\b", re.I)
SENIOR_RE = re.compile(
    r"\b(senior|sr\.?|staff|principal|distinguished|fellow|lead|head of|manager|gestionnaire|director|directeur|"
    r"directrice|vp|vice president|architect|architecte|intermediate|interm[ée]diaire|mid[- ]level|mid[- ]senior|"
    r"expert|supervisor|superviseur|chef d'[ée]quipe|iii|iv|level [3-9]|l[4-9]|niveau [3-9]|10\+|[5-9]\+ ?(years|yrs|ans))\b",
    re.I)
NONENG_RE = re.compile(
    r"\b(sales|account executive|account manager|marketing|recruit|talent acquisition|human resources|payroll|"
    r"accountant|accounting|comptable|legal|counsel|paralegal|nurse|driver|chauffeur|warehouse|janitor|custodian|"
    r"security guard|receptionist|administrative|customer service|buyer|acheteur|procurement|purchasing|inventory|"
    r"assembler|assembleur|welder|soudeur|machinist|machiniste|forklift|dispatcher|cashier|cook|electrician|"
    r"plumber|millwright|articling|communications|brand|social media|event|treasury|tax|audit|actuar|underwrit|"
    r"claims|mortgage|banking advisor|teller|physician|pharmac|dental|clinical|therapist|professor|faculty|lecturer|postdoc|clerk|coordinator|coordonn|planner|planificateur|scheduler|recruiter|actuar|consultant|proposal|administrator|admin|venture|fund|orthop|registered|licensed practical|caregiver|teacher|tutor|librarian|chaplain|barista|server|host|guard|cleaner|labou?rer|painter|carpenter|mechanic\b|installer|field service|technicien|technician(?! engineer))\b", re.I)
INTERN_RE = re.compile(
    r"\b(intern|internship|co-?op|stagiaire|stage|student|[ée]tudiant(e)?|summer 20\d\d|winter 20\d\d|fall 20\d\d|"
    r"spring 20\d\d|\bPEY\b|work term|placement)\b", re.I)
NEWGRAD_RE = re.compile(
    r"(new[- ]grad|new graduate|nouveaux? dipl[ôo]m|recent graduate|r[ée]cent(e)?s? dipl[ôo]m|graduating|class of 2027|"
    r"\b2027\b|entry[- ]level|premier [ée]chelon|junior|\bjr\.?\b|associate|engineer i\b|engineer 1\b|developer i\b|"
    r"scientist i\b|level 1\b|\bL1\b|\bEIT\b|engineer[- ]in[- ]training|ing[ée]nieur(e)? stagiaire|\bCPI\b|"
    r"graduate (engineer|program|programme|development|trainee|rotation|scheme)|rotational|leadership development|"
    r"early[- ]career|early talent|campus|university grad|university recruiting|trainee|apprenti|d[ée]butant|"
    r"nouvel(le)? dipl|0-[23] years|0 to [23] years)", re.I)
DOMAIN_RE = re.compile(
    r"(robot|autonom|self[- ]driving|driverless|perception|computer vision|\bvision\b|machine learning|\bML\b|\bAI\b|"
    r"artificial intelligence|deep learning|reinforcement|\bcontrol|\bGNC\b|guidance|navigation|motion|planning|"
    r"embedded|firmware|\bFPGA\b|\bDSP\b|mechatronic|m[ée]catronique|systems? engineer|ing[ée]nieur(e)? syst|"
    r"test engineer|validation|verification|v[ée]rification|simulation|hardware|electrical|[ée]lectrique|electronic|"
    r"[ée]lectronique|signal processing|sensor|capteur|lidar|radar|camera|\bSLAM\b|localization|mapping|research|"
    r"recherche|scientist|scientifique|data scien|applied scien|software (engineer|developer)|d[ée]veloppeu(r|se)|"
    r"ing[ée]nieur|engineer|aerospace|a[ée]rospatial|avionic|propulsion|automation|automatisation|instrumentation|"
    r"\bI&C\b|power system|nuclear|nucl[ée]aire|\brail|signalling|signaling|vehicle|v[ée]hicule|automotive|\bADAS\b|"
    r"\bEV\b|battery|batterie|drone|\bUAV\b|\bspace\b|satellite|quantum|optic|photonic|semiconductor|\bASIC\b|"
    r"\bRTL\b|silicon|compiler|\bGPU\b|\bML\b|analytics|data engineer|technologist|technologue|mechanical|m[ée]canique)",
    re.I)


OFFFIELD_RE = re.compile(
    r"(civil|structural|geotech|geolog|ecolog|environmental|hydrogeolog|hydrolog|mining|mineral|metallurg|refrigeration|"
    r"HVAC|piping|plumbing|petroleum|chemical process|process engineer|water resources|wastewater|transportation planning|"
    r"traffic|surveying|architectural|building science|construction|contracts?\b|supply chain|logistics|business analyst|"
    r"\bDBA\b|database administrator|\bSAP\b|salesforce|\bCRM\b|\bERP\b|clearing|actuar|\btax\b|audit|cost controller|"
    r"land development|municipal|bridge|roadway|highway|pavement|GIS analyst|urban|die process|tooling|weld|"
    r"maintenance|facilities|quality inspector|production|manufacturing engineer|industrial engineer|reliability engineer)", re.I)
INSCOPE_RE = re.compile(
    r"(robot|autonom|\bcontrols?\b|embedded|firmware|perception|vision|machine learning|\bML\b|\bAI\b|mechatronic|automation|"
    r"software|electrical|electronic|instrument|simulation|\btest\b|validation|systems? engineer|signal|sensor|aerospace|"
    r"avionic|propulsion|research|scientist|data scien|\bGNC\b|navigation|hardware|FPGA|ASIC|nuclear|I&C|rotation|"
    r"graduate program|new grad|leadership development|EIT\b|engineer[- ]in[- ]training)", re.I)
NG_STRICT_RE = re.compile(
    r"(new[- ]grad|new graduate|nouveaux? dipl[ôo]m|recent graduate|graduate (program|programme|trainee|rotation|scheme|engineer)|"
    r"rotational|early[- ]career|entry[- ]level|\bEIT\b|engineer[- ]in[- ]training|leadership development|\bCPI\b)", re.I)
EXPLICIT_NG_RE = re.compile(
    r"(new[- ]grad|new graduate|nouveaux? dipl[ôo]m|recent graduate|r[ée]cent(e)?s? dipl[ôo]m|entry[- ]level|"
    r"premier [ée]chelon|\bEIT\b|engineer[- ]in[- ]training|graduate (engineer|program|programme|development|trainee|"
    r"rotation|scheme)|rotational|leadership development|early[- ]career|university grad|junior|\bjr\.?\b|associate)", re.I)
TITLE_LOC_RE = re.compile(r"\(([^()]{3,80}(?:,[^()]{1,40}){1,4})\)\s*$")


def split_title_location(title):
    """'Junior Engineer (Dorval, QC, CA)' -> ('Junior Engineer', 'Dorval, QC, CA')."""
    m = TITLE_LOC_RE.search(title or "")
    if m and (LOC_RE.search(m.group(1)) or OUTSIDE_RE.search(m.group(1)) or re.search(r"\b[A-Z]{2}\b", m.group(1))):
        return title[: m.start()].strip(" -,"), m.group(1).strip()
    return title, ""


def classify(it, employer_source):
    """Return (keep: bool, reason: str, flags: list)."""
    t = it.get("title", "") or ""
    loc = it.get("location", "") or ""
    text = (t + " " + (it.get("snippet", "") or ""))
    flags = []
    if not t or len(t) < 4:
        return False, "no_title", flags
    if NONENG_RE.search(t) and not re.search(r"engineer|ing[ée]nieur|developer|scientist|robot|automation", t, re.I):
        return False, "non_engineering", flags
    if SENIOR_RE.search(t):
        return False, "senior", flags
    if re.search(r"\bII\b", t) and not EXPLICIT_NG_RE.search(t):
        return False, "senior", flags
    if INTERN_RE.search(t) and not NG_STRICT_RE.search(t):
        return False, "internship", flags
    if OFFFIELD_RE.search(t) and not INSCOPE_RE.search(t):
        return False, "off_field", flags
    in_onqc = bool(LOC_RE.search(loc))
    outside = bool(OUTSIDE_RE.search(loc))
    if outside and not in_onqc:
        return False, "outside_onqc", flags
    if not in_onqc:
        if employer_source:
            if loc and not it.get("loc_default") and not re.search(r"canada|remote|locations|hybrid|multiple|various|home", loc, re.I):
                return False, "outside_onqc", flags
            flags.append("loc_unconfirmed")
        elif loc and not re.search(r"canada|remote", loc, re.I):
            return False, "outside_onqc", flags
        else:
            flags.append("loc_unconfirmed")
    if not (DOMAIN_RE.search(text) or NEWGRAD_RE.search(text)):
        return False, "off_domain", flags
    if re.search(r"too many feeds|no results found", t, re.I):
        return False, "pseudo_item", flags
    if not employer_source and it.get("posted"):
        try:
            if (TODAY - dt.date.fromisoformat(it["posted"])).days > 60:
                return False, "stale", flags
        except Exception:
            pass
    if NEWGRAD_RE.search(t):
        flags.append("entry_wording")
    if re.search(r"bilingu|fran[çc]ais|french", text, re.I):
        flags.append("FR?")
    return True, "ok", flags


def seen_key(it):
    u = canon_url(it.get("url", ""))
    if u:
        return u
    base = "|".join([(it.get("company") or "").lower(), (it.get("title") or "").lower(), (it.get("location") or "").lower()])
    return "h:" + hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


# ----------------------------------------------------------------------------
# Generic extractors
# ----------------------------------------------------------------------------

TITLE_KEYS = ("title", "jobTitle", "publishedJobTitle", "name", "text", "postingTitle", "jobOpeningName", "Title",
              "position_title", "job_title", "positionTitle", "displayName", "label")
URL_KEYS = ("url", "absolute_url", "hostedUrl", "jobUrl", "careers_url", "apply_url", "link", "canonicalPositionUrl",
            "applyUrl", "href", "job_url", "jobUrl", "detailUrl", "permalink", "web_url", "shareUrl", "externalPath")
LOC_KEYS = ("location", "locationsText", "PrimaryLocation", "city", "locations", "workLocation", "location_name",
            "office", "region", "locationName", "primaryLocation", "displayLocation", "siteLocation")
DATE_KEYS = ("posted", "postedOn", "PostedDate", "publishedAt", "published_at", "published", "createdAt", "created_at",
             "first_published", "updated_at", "releasedDate", "date_posted", "postDateInGMT", "posted_date",
             "t_update", "t_create", "datePosted", "pubDate", "updatedAt", "dateUpdated", "date")


def _loc_from(v):
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        parts = [str(v.get(k)) for k in ("name", "city", "region", "state", "province", "country", "locationName", "displayName") if v.get(k)]
        return ", ".join(parts)
    if isinstance(v, list):
        return "; ".join(_loc_from(x) for x in v[:3])
    return str(v)


def json_generic(obj, base_url, src_name, depth=0, out=None):
    """Recursively harvest dicts that look like job postings."""
    if out is None:
        out = []
    if depth > 8:
        return out
    if isinstance(obj, dict):
        title = next((obj[k] for k in TITLE_KEYS if isinstance(obj.get(k), str) and 3 < len(obj[k]) < 200), None)
        url = next((obj[k] for k in URL_KEYS if isinstance(obj.get(k), str) and obj[k]), None)
        if title and (url or obj.get("id") or obj.get("Id")):
            loc = next((_loc_from(obj[k]) for k in LOC_KEYS if obj.get(k)), "")
            posted = next((obj[k] for k in DATE_KEYS if obj.get(k)), None)
            snippet = next((obj[k] for k in ("description", "descriptionPlain", "summary", "content", "jobDescription", "bulletFields", "department", "departmentLabel", "team") if obj.get(k)), "")
            if isinstance(snippet, list):
                snippet = " ".join(str(x) for x in snippet)
            u = abs_url(base_url, url) if url else ""
            out.append(item(title, u, loc, posted, snippet, "", src_name))
            return out
        for v in obj.values():
            json_generic(v, base_url, src_name, depth + 1, out)
    elif isinstance(obj, list):
        for v in obj:
            json_generic(v, base_url, src_name, depth + 1, out)
    return out


A_RE = re.compile(r'<a\b([^>]*)href="([^"]+)"([^>]*)>(.*?)</a>', re.I | re.S)
JOBISH_RE = re.compile(r"(engineer|ing[ée]nieur|developer|d[ée]veloppeu|scientist|analyst|technolog|specialist|"
                       r"intern|co-?op|stagiaire|graduate|dipl[ôo]m|associate|junior|robot|research|designer|"
                       r"programmer|architect|manager|lead|coordinator|technician|technicien|EIT|trainee)", re.I)
NAV_RE = re.compile(r"(login|sign in|privacy|cookie|terms|contact|about|home|menu|share|facebook|twitter|linkedin\.com/company|"
                    r"instagram|youtube|apply now$|read more|learn more|view all|see all|back to)", re.I)


def html_generic(text, base_url, src_name, must_jobish=True):
    out = []
    seen_local = set()
    for m in A_RE.finditer(text):
        href = m.group(2)
        inner = strip_tags(m.group(4), 200)
        if not inner or len(inner) < 6 or len(inner) > 160:
            continue
        if href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        if NAV_RE.search(inner) and not JOBISH_RE.search(inner):
            continue
        if must_jobish and not JOBISH_RE.search(inner):
            continue
        u = abs_url(base_url, href)
        if u in seen_local:
            continue
        seen_local.add(u)
        # location: inside the anchor text first, then nearby text after the anchor
        loc = ""
        title = inner
        im = LOC_RE.search(inner) or OUTSIDE_RE.search(inner)
        if im and im.start() > 3:
            title = inner[: im.start()].strip(" -|,(")
            loc = inner[im.start():].strip(" )")
        else:
            tail = strip_tags(text[m.end():m.end() + 600], 300)
            lm = LOC_RE.search(tail) or OUTSIDE_RE.search(tail)
            if lm:
                loc = tail[max(0, lm.start() - 25): lm.end() + 25]
        out.append(item(title, u, loc, None, "", "", src_name))
    return out


# ----------------------------------------------------------------------------
# ATS parsers (employer sources)
# ----------------------------------------------------------------------------

def p_greenhouse(src):
    j, err = get_json(src["url"])
    if j is None:
        return [], err
    out = []
    for job in j.get("jobs", []):
        loc = (job.get("location") or {}).get("name", "")
        offices = "; ".join(o.get("location", "") or o.get("name", "") for o in job.get("offices", []) if isinstance(o, dict))
        out.append(item(job.get("title"), job.get("absolute_url"), loc or offices,
                        job.get("first_published") or job.get("updated_at"), job.get("content", ""), "", src["name"]))
    return out, None


def p_lever(src):
    j, err = get_json(src["url"])
    if j is None:
        return [], err
    out = []
    for job in j if isinstance(j, list) else []:
        cat = job.get("categories") or {}
        loc = cat.get("location", "") or ""
        if job.get("country"):
            loc = "%s, %s" % (loc, job["country"]) if loc else job["country"]
        snippet = "%s %s %s" % (cat.get("commitment", ""), cat.get("team", ""), (job.get("descriptionPlain") or "")[:300])
        out.append(item(job.get("text"), job.get("hostedUrl"), loc, job.get("createdAt"), snippet, "", src["name"]))
    return out, None


def p_ashby(src):
    j, err = get_json(src["url"])
    if j is None:
        return [], err
    out = []
    for job in j.get("jobs", []):
        loc = job.get("location", "") or ""
        sec = job.get("secondaryLocations") or []
        if sec:
            loc += "; " + "; ".join(s.get("location", "") for s in sec if isinstance(s, dict))
        out.append(item(job.get("title"), job.get("jobUrl"), loc, job.get("publishedAt"),
                        "%s %s" % (job.get("employmentType", ""), job.get("department", "")), "", src["name"]))
    return out, None


def p_smartrecruiters(src):
    j, err = get_json(src["url"])
    if j is None:
        return [], err
    company = re.search(r"/companies/([^/]+)/", src["url"])
    company = company.group(1) if company else ""
    out = []
    for job in j.get("content", []):
        loc = job.get("location") or {}
        locs = ", ".join(str(loc.get(k)) for k in ("city", "region", "country") if loc.get(k))
        url = "https://jobs.smartrecruiters.com/%s/%s" % (company, job.get("id"))
        out.append(item(job.get("name"), url, locs, job.get("releasedDate"), (job.get("department") or {}).get("label", ""), "", src["name"]))
    return out, None


def p_workable_v3(src):
    account = src["url"].rstrip("/").split("/accounts/")[1].split("/")[0]
    j, err = get_json(src["url"], "POST", src.get("body") or {"query": "", "location": [], "department": [], "worktype": [], "remote": []})
    if j is None:
        return [], err
    out = []
    for job in j.get("results", []):
        loc = job.get("location") or {}
        locs = ", ".join(str(loc.get(k)) for k in ("city", "region", "country") if loc.get(k))
        url = "https://apply.workable.com/%s/j/%s/" % (account, job.get("shortcode"))
        out.append(item(job.get("title"), url, locs, job.get("published"), job.get("department", ""), "", src["name"]))
    return out, None


def p_recruitee(src):
    j, err = get_json(src["url"])
    if j is None:
        return [], err
    out = []
    for job in j.get("offers", []):
        loc = ", ".join(str(job.get(k)) for k in ("city", "state_name", "country") if job.get(k))
        out.append(item(job.get("title"), job.get("careers_url"), loc, job.get("published_at"), job.get("department", ""), "", src["name"]))
    return out, None


def p_bamboohr(src):
    j, err = get_json(src["url"])
    if j is None:
        return [], err
    sub = urllib.parse.urlsplit(src["url"]).netloc
    out = []
    for job in j.get("result", []):
        loc = job.get("location") or {}
        locs = ", ".join(str(loc.get(k)) for k in ("city", "state", "country") if isinstance(loc, dict) and loc.get(k))
        url = "https://%s/careers/%s" % (sub, job.get("id"))
        out.append(item(job.get("jobOpeningName"), url, locs, None, job.get("departmentLabel", ""), "", src["name"]))
    return out, None


def p_workday(src):
    url = src["url"]
    m = re.match(r"(https://[^/]+)/wday/cxs/([^/]+)/([^/]+)/jobs", url)
    if not m:
        return [], "bad workday url"
    host, tenant, site = m.group(1), m.group(2), m.group(3)
    terms = src.get("search_terms") or ["engineer", "new grad", "junior", "associate"]
    facets = src.get("body_facets") or {}
    out, errs = [], []
    for term in terms:
        for off in (0, 20, 40):
            body = {"appliedFacets": facets, "limit": 20, "offset": off, "searchText": term}
            j, err = get_json(url, "POST", body, {"Accept": "application/json"})
            if j is None:
                errs.append("%s: %s" % (term, err))
                break
            posts = j.get("jobPostings", []) or []
            for job in posts:
                ext = job.get("externalPath", "")
                out.append(item(job.get("title"), "%s/%s%s" % (host, site, ext), job.get("locationsText", ""),
                                job.get("postedOn"), " ".join(job.get("bulletFields", []) or []), "", src["name"]))
            if len(posts) < 20:
                break
    return out, ("; ".join(errs) if errs and not out else None)


def p_oracle_hcm(src):
    j, err = get_json(src["url"], headers={"Accept": "application/json"})
    if j is None:
        return [], err
    host = "https://" + urllib.parse.urlsplit(src["url"]).netloc
    sm = re.search(r"siteNumber=([A-Za-z0-9_]+)", src["url"])
    site = sm.group(1) if sm else "CX_1"
    out = []
    try:
        reqs = j["items"][0].get("requisitionList", [])
    except Exception:
        return json_generic(j, host, src["name"]), None
    for r in reqs:
        loc = r.get("PrimaryLocation", "")
        sec = r.get("secondaryLocations") or []
        if sec:
            loc += "; " + "; ".join(s.get("Name", "") for s in sec if isinstance(s, dict))
        url = "%s/hcmUI/CandidateExperience/en/sites/%s/job/%s" % (host, site, r.get("Id"))
        out.append(item(r.get("Title"), url, loc, r.get("PostedDate"), r.get("ShortDescriptionStr", ""), "", src["name"]))
    return out, None


def p_icims_json(src):
    j, err = get_json(src["url"])
    if j is None:
        return [], err
    out = []
    for job in j.get("jobs", []):
        d = job.get("data", job) if isinstance(job, dict) else {}
        loc = ", ".join(str(d.get(k)) for k in ("city", "state", "country") if d.get(k))
        posted = next((d[k] for k in ("posted_date", "postedDate", "create_date", "updateDate") if d.get(k)), None)
        out.append(item(d.get("title"), d.get("apply_url") or d.get("url"), loc, posted, d.get("description", "")[:300] if d.get("description") else "", "", src["name"]))
    return out, None


def p_eightfold_pcsx(src):
    urls = [src["url"]] + list(src.get("extra_urls") or [])
    out, errs = [], []
    for u in urls:
        j, err = get_json(u)
        if j is None:
            errs.append(err)
            continue
        host = "https://" + urllib.parse.urlsplit(u).netloc
        positions = (j.get("data") or {}).get("positions") if isinstance(j.get("data"), dict) else None
        if positions is None:
            positions = j.get("positions", [])
        for p in positions or []:
            locs = p.get("locations") or p.get("standardizedLocations") or p.get("location") or ""
            if isinstance(locs, list):
                locs = "; ".join(str(x) for x in locs)
            purl = p.get("canonicalPositionUrl") or p.get("positionUrl") or ""
            out.append(item(p.get("name"), abs_url(host, purl), locs, p.get("postedTs") or p.get("t_update") or p.get("creationTs"),
                            "%s %s" % (p.get("department", ""), p.get("workLocationOption", "")), "", src["name"]))
    return out, ("; ".join(errs) if errs and not out else None)


def p_amazon(src):
    j, err = get_json(src["url"])
    if j is None:
        return [], err
    out = []
    for job in j.get("jobs", []):
        out.append(item(job.get("title"), "https://www.amazon.jobs" + (job.get("job_path") or ""), job.get("normalized_location") or job.get("location", ""),
                        job.get("posted_date"), job.get("description_short", ""), "", src["name"]))
    return out, None


def p_apple(src):
    j, err = get_json(src["url"], "POST", src.get("body"), {"Accept": "application/json"})
    if j is None:
        return [], err
    out = []
    for r in j.get("searchResults", []):
        locs = "; ".join(l.get("name", "") for l in r.get("locations", []) if isinstance(l, dict))
        url = "https://jobs.apple.com/en-ca/details/%s" % r.get("positionId")
        out.append(item(r.get("postingTitle"), url, locs, r.get("postDateInGMT"), r.get("team", {}).get("teamName", "") if isinstance(r.get("team"), dict) else "", "", src["name"]))
    return out, None


def _rss_items(text, base, src_name):
    out = []
    try:
        root = ET.fromstring(text.encode("utf-8"))
    except Exception:
        return out
    for it in root.iter():
        tag = it.tag.split("}")[-1].lower()
        if tag not in ("item", "entry"):
            continue
        d = {}
        for c in it:
            ctag = c.tag.split("}")[-1].lower()
            if ctag == "link":
                d["link"] = (c.text or "").strip() or c.get("href", "")
            elif c.text and ctag not in d:
                d[ctag] = c.text.strip()
        title = d.get("title", "")
        desc = d.get("description") or d.get("summary") or d.get("content") or ""
        loc = d.get("location") or d.get("city") or ""
        title, tloc = split_title_location(title)
        if tloc:
            loc = tloc
        if not loc:
            plain = strip_tags(desc, 800)
            lm = LOC_RE.search(plain)
            if lm:
                seg = plain[max(0, lm.start() - 40): lm.end() + 40]
                loc = re.sub(r"^\S*\s", "", seg).strip()
        out.append(item(title, abs_url(base, d.get("link", "")), loc, d.get("pubdate") or d.get("published") or d.get("updated"),
                        desc, d.get("employer") or d.get("company") or "", src_name))
    return out


def p_successfactors_rss(src):
    st, txt = http(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    return _rss_items(txt, src["url"], src["name"]), None


SF_ROW_RE = re.compile(r'<a[^>]+class="[^"]*jobTitle-link[^"]*"[^>]+href="([^"]+)"[^>]*>(.*?)</a>(.*?)(?=<a[^>]+class="[^"]*jobTitle-link|</table>|$)', re.I | re.S)


def p_successfactors_html(src):
    out, errs = [], []
    for start in (0, 25, 50):
        u = src["url"] + ("&" if "?" in src["url"] else "?") + "startrow=%d" % start
        st, txt = http(u)
        if st is None or st >= 400:
            errs.append("HTTP %s" % st)
            break
        n = 0
        for m in SF_ROW_RE.finditer(txt):
            n += 1
            tail = m.group(3)
            lm = re.search(r'class="[^"]*jobLocation[^"]*"[^>]*>(.*?)</', tail, re.I | re.S)
            dm = re.search(r'class="[^"]*jobDate[^"]*"[^>]*>(.*?)</', tail, re.I | re.S)
            out.append(item(strip_tags(m.group(2)), abs_url(u, m.group(1)), strip_tags(lm.group(1)) if lm else "",
                            strip_tags(dm.group(1)) if dm else None, "", "", src["name"]))
        if n < 25:
            break
    if not out and not errs:
        # fall back to generic anchors
        st, txt = http(src["url"])
        if st and st < 400:
            out = [x for x in html_generic(txt, src["url"], src["name"]) if "/job/" in x["url"]]
    return out, ("; ".join(errs) if errs and not out else None)


def p_sitemap_slug(src):
    st, txt = http(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    out = []
    for m in re.finditer(r"<loc>\s*([^<\s]+)\s*</loc>", txt):
        u = m.group(1)
        parts = [p for p in urllib.parse.urlsplit(u).path.split("/") if p]
        if len(parts) < 2:
            continue
        city = parts[0].replace("-", " ")
        title = parts[1].replace("-", " ")
        if not LOC_RE.search(city):
            continue
        out.append(item(title.title(), u, city, None, "", "", src["name"]))
    return out, None


def p_radancy_json(src):
    """Radancy/TalentBrew search-results endpoint: JSON whose 'results' field is an HTML fragment."""
    j, err = get_json(src["url"], headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
    if j is None:
        return [], err
    frag = j.get("results") or j.get("html") or ""
    if not isinstance(frag, str):
        return [], "no html fragment"
    base = "https://" + urllib.parse.urlsplit(src["url"]).netloc
    return [x for x in html_generic(frag, base, src["name"]) if "/job/" in x["url"]], None


def p_adp_workforcenow(src):
    j, err = get_json(src["url"], headers={"Accept": "application/json"})
    if j is None:
        return [], err
    q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(src["url"]).query))
    out = []
    for r in j.get("jobRequisitions", []):
        jid = r.get("itemID") or ""
        url = ("https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=%s&ccId=%s&jobId=%s&lang=en_CA"
               % (q.get("cid", ""), q.get("ccId", ""), jid))
        loc = _loc_from(r.get("requisitionLocations") or r.get("primaryLocation") or "")
        desc = r.get("requisitionDescription") or ""
        out.append(item(r.get("requisitionTitle"), url, loc, r.get("postDate"), desc[:300], "", src["name"]))
    return out, None


def p_json_generic(src):
    j, err = get_json(src["url"], src.get("method", "GET"), src.get("body"), src.get("headers"))
    if j is None:
        return [], err
    return json_generic(j, src["url"], src["name"]), None


def p_html_generic(src):
    st, txt = http(src["url"], src.get("method", "GET"), src.get("body"), src.get("headers"))
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    return html_generic(txt, src["url"], src["name"]), None


PARSERS = {
    "greenhouse": p_greenhouse, "lever": p_lever, "ashby": p_ashby, "smartrecruiters": p_smartrecruiters,
    "workable_v3": p_workable_v3, "recruitee": p_recruitee, "bamboohr": p_bamboohr, "workday": p_workday,
    "oracle_hcm": p_oracle_hcm, "icims_json": p_icims_json, "eightfold_pcsx": p_eightfold_pcsx, "amazon": p_amazon,
    "apple": p_apple, "successfactors_rss": p_successfactors_rss, "successfactors_html": p_successfactors_html,
    "sitemap_slug": p_sitemap_slug, "json_generic": p_json_generic, "html_generic": p_html_generic,
    "radancy_json": p_radancy_json, "adp_workforcenow": p_adp_workforcenow,
}


def run_employer(src):
    t0 = time.time()
    ats = src.get("ats", "none")
    if ats == "none" or not src.get("url"):
        return src["name"], [], {"status": "skipped", "n": 0, "secs": 0}
    fn = PARSERS.get(ats)
    if not fn:
        return src["name"], [], {"status": "no_parser:" + ats, "n": 0, "secs": 0}
    try:
        items, err = fn(src)
    except Exception as e:
        items, err = [], "%s: %s" % (type(e).__name__, e)
    default_loc = ", ".join(src.get("cities", [])[:2])
    for it in items:
        it["company"] = it.get("company") or src["name"]
        it["employer_source"] = True
        it["sector"] = src.get("sector", "")
        it["clearance_risk"] = src.get("clearance_risk", "")
        it["french_likely"] = bool(src.get("french_likely"))
        t2, tloc = split_title_location(it.get("title", ""))
        if tloc:
            it["title"] = t2
            if not it.get("location") or not (LOC_RE.search(it["location"]) or OUTSIDE_RE.search(it["location"])):
                it["location"] = tloc
        if not it.get("location"):
            it["location"] = default_loc
            it["loc_default"] = True
    status = "ok" if items else ("error: %s" % err if err else "empty")
    return src["name"], items, {"status": status, "n": len(items), "secs": round(time.time() - t0, 1)}


# ----------------------------------------------------------------------------
# Aggregators
# ----------------------------------------------------------------------------

LI_CARD_RE = re.compile(r'data-entity-urn="urn:li:jobPosting:(\d+)"(.*?)(?=data-entity-urn="urn:li:jobPosting:|$)', re.S)


def run_linkedin(cfg, skip):
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    if skip or not cfg.get("enabled", True):
        stats["status"] = "skipped"
        return [], stats
    out = []
    pace = float(cfg.get("pace_seconds", 3.5))
    for kw in cfg.get("keywords", []):
        for geo_name, geo in cfg.get("geo_ids", {}).items():
            for start in cfg.get("pages", [0]):
                u = ("https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=%s&geoId=%s&f_TPR=r604800&start=%d"
                     % (urllib.parse.quote(kw), geo, start))
                st, txt = http(u, headers={"Accept": "text/html"})
                stats["requests"] += 1
                if st == 429:
                    stats["errors"].append("429 at %s/%s; backing off 60s" % (kw, geo_name))
                    time.sleep(60)
                    st, txt = http(u, headers={"Accept": "text/html"})
                if st is None or st >= 400:
                    stats["errors"].append("%s %s/%s: %s" % (st, kw, geo_name, txt[:60] if st is None else ""))
                    time.sleep(pace)
                    break
                n = 0
                for m in LI_CARD_RE.finditer(txt):
                    n += 1
                    jid, block = m.group(1), m.group(2)
                    tm = re.search(r'<h3[^>]*base-search-card__title[^>]*>(.*?)</h3>', block, re.S)
                    cm = re.search(r'<h4[^>]*base-search-card__subtitle[^>]*>(.*?)</h4>', block, re.S)
                    lm = re.search(r'<span[^>]*job-search-card__location[^>]*>(.*?)</span>', block, re.S)
                    dm = re.search(r'<time[^>]*datetime="([^"]+)"', block)
                    am = re.search(r'<a[^>]*class="base-card__full-link"[^>]*href="([^"]+)"', block)
                    url = am.group(1).split("?")[0] if am else "https://www.linkedin.com/jobs/view/%s" % jid
                    out.append(item(strip_tags(tm.group(1)) if tm else "", url, strip_tags(lm.group(1)) if lm else geo_name,
                                    dm.group(1) if dm else None, "", strip_tags(cm.group(1)) if cm else "", "linkedin", {"query": kw}))
                time.sleep(pace)
                if n < 10:
                    break
    stats["n"] = len(out)
    return out, stats


def run_eluta(cfg, skip):
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    if skip or not cfg.get("enabled", True):
        stats["status"] = "skipped"
        return [], stats
    out = []
    for q in cfg.get("queries", []):
        for place in cfg.get("places", []):
            u = "https://www.eluta.ca/rss?q=%s&l=%s" % (urllib.parse.quote(q + " sort:date"), urllib.parse.quote(place))
            st, txt = http(u)
            stats["requests"] += 1
            if st is None or st >= 400:
                stats["errors"].append("%s %s/%s" % (st, q, place))
                continue
            time.sleep(1.0)
            for it in _rss_items(txt, u, "eluta"):
                it["query"] = q
                if not it["location"]:
                    it["location"] = place
                out.append(it)
            time.sleep(0.5)
    stats["n"] = len(out)
    return out, stats


def run_jobbank(cfg, skip):
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    if skip or not cfg.get("enabled", True):
        stats["status"] = "skipped"
        return [], stats
    out = []
    pace = float(cfg.get("pace_seconds", 5))
    for q in cfg.get("queries", []):
        for prov in cfg.get("provinces", ["ON", "QC"]):
            u = ("https://www.jobbank.gc.ca/jobsearch/feed/jobSearchRSSfeed?searchstring=%s&fprov=%s&sort=D&rows=50&fage=7"
                 % (urllib.parse.quote(q), prov))
            st, txt = http(u)
            stats["requests"] += 1
            if st is None or st >= 400:
                stats["errors"].append("%s %s/%s" % (st, q, prov))
            else:
                for it in _rss_items(txt, u, "jobbank"):
                    it["query"] = q
                    if not LOC_RE.search(it["location"] or ""):
                        it["location"] = (it["location"] + " " if it["location"] else "") + ("Ontario" if prov == "ON" else "Quebec")
                    out.append(it)
            time.sleep(pace)
    stats["n"] = len(out)
    return out, stats


def run_vector(cfg, skip):
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    if skip or not cfg.get("enabled", True):
        stats["status"] = "skipped"
        return [], stats
    out = []
    for t in cfg.get("terms", []):
        u = "https://talenthub.vectorinstitute.ai/jobs/search.rss?q=%s" % urllib.parse.quote(t)
        st, txt = http(u)
        stats["requests"] += 1
        if st is None or st >= 400:
            stats["errors"].append("%s %s" % (st, t))
            continue
        for it in _rss_items(txt, u, "vector_talent_hub"):
            it["query"] = t
            out.append(it)
    stats["n"] = len(out)
    return out, stats


def run_getro(cfg, skip):
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    if skip or not cfg.get("enabled", True):
        stats["status"] = "skipped"
        return [], stats
    out = []
    for cname, cid in cfg.get("collections", {}).items():
        for t in cfg.get("terms", []):
            u = "https://api.getro.com/api/v2/collections/%s/search/jobs" % cid
            j, err = get_json(u, "POST", {"hitsPerPage": 50, "page": 0, "query": t},
                              {"Origin": "https://www1.communitech.ca", "Referer": "https://www1.communitech.ca/jobs", "Accept": "application/json"})
            stats["requests"] += 1
            if j is None:
                stats["errors"].append("%s %s: %s" % (cname, t, err))
                continue
            hits = j.get("results") or j.get("hits") or (j.get("data") or {}).get("results") or []
            if isinstance(hits, dict):
                hits = hits.get("jobs") or hits.get("hits") or []
            for h in hits:
                if not isinstance(h, dict):
                    continue
                org = h.get("organization") or {}
                locs = h.get("searchable_locations") or h.get("locations") or h.get("location") or ""
                out.append(item(h.get("title"), h.get("url") or h.get("apply_url") or h.get("careers_url") or "",
                                _loc_from(locs), h.get("created_at") or h.get("published_at"), "",
                                org.get("name", "") if isinstance(org, dict) else str(org), "getro:" + cname))
    stats["n"] = len(out)
    return out, stats


def run_hn(cfg, skip):
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    if skip or not cfg.get("enabled", True):
        stats["status"] = "skipped"
        return [], stats
    out = []
    cutoff = int((dt.datetime.utcnow() - dt.timedelta(days=45)).timestamp())
    for q in cfg.get("queries", []):
        u = "https://hn.algolia.com/api/v1/search?query=%s&tags=comment&hitsPerPage=50&numericFilters=created_at_i>%d" % (urllib.parse.quote(q), cutoff)
        j, err = get_json(u)
        stats["requests"] += 1
        if j is None:
            stats["errors"].append("%s: %s" % (q, err))
            continue
        for h in j.get("hits", []):
            txt = strip_tags(h.get("comment_text", ""), 600)
            if not LOC_RE.search(txt):
                continue
            title = txt.split("|")[0][:120]
            url = "https://news.ycombinator.com/item?id=%s" % h.get("objectID")
            out.append(item("HN: " + title, url, "see text", h.get("created_at"), txt, "", "hn_whoishiring"))
    stats["n"] = len(out)
    return out, stats


def run_ros(cfg, skip):
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    if skip or not cfg.get("enabled", True):
        stats["status"] = "skipped"
        return [], stats
    out = []
    j, err = get_json(cfg.get("url", "https://discourse.openrobotics.org/c/jobs/15.json"))
    stats["requests"] = 1
    if j is None:
        stats["errors"].append(err)
        stats["status"] = "error"
        return out, stats
    for t in (j.get("topic_list") or {}).get("topics", []):
        title = t.get("title", "")
        if not re.search(r"canada|ontario|toronto|montr|quebec|ottawa|waterloo|kitchener", title, re.I):
            continue
        url = "https://discourse.openrobotics.org/t/%s/%s" % (t.get("slug"), t.get("id"))
        out.append(item(title, url, "see title", t.get("created_at"), "", "", "ros_discourse"))
    stats["n"] = len(out)
    return out, stats


MD_LINK_RE = re.compile(r"\((https?://[^)\s]+)\)|href=\"(https?://[^\"]+)\"")


def parse_md_table(text, src_name):
    out = []
    header_map = None
    company_carry = ""
    for line in text.splitlines():
        if not line.strip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if all(re.match(r"^:?-+:?$", c) for c in cells if c):
            continue
        low = [strip_tags(re.sub(r"[^A-Za-z ]", " ", c)).lower() for c in cells]
        if header_map is None or any(k in " ".join(low) for k in ("company", "role", "location")) and any("company" in c for c in low):
            hm = {}
            for i, c in enumerate(low):
                if "company" in c and "company" not in hm:
                    hm["company"] = i
                elif any(k in c for k in ("role", "title", "position", "job")) and "role" not in hm:
                    hm["role"] = i
                elif "location" in c and "location" not in hm:
                    hm["location"] = i
                elif any(k in c for k in ("link", "apply", "application", "url")) and "link" not in hm:
                    hm["link"] = i
                elif any(k in c for k in ("date", "age", "posted")) and "date" not in hm:
                    hm["date"] = i
            if "company" in hm and "role" in hm:
                header_map = hm
                continue
        if header_map is None:
            continue
        def cell(k):
            i = header_map.get(k)
            return cells[i] if i is not None and i < len(cells) else ""
        company = strip_tags(re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", cell("company"))).strip("*_ ")
        if company in ("", "↳", "↳"):
            company = company_carry
        else:
            company_carry = company
        role = strip_tags(re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", cell("role"))).strip("*_ ")
        loc = strip_tags(cell("location"))
        link_cell = cell("link") or line
        lm = MD_LINK_RE.search(link_cell) or MD_LINK_RE.search(line)
        url = (lm.group(1) or lm.group(2)) if lm else ""
        if "simplify.jobs" in url:
            alt = [u for u in re.findall(r"\((https?://[^)\s]+)\)", line) if "simplify.jobs" not in u]
            if alt:
                url = alt[0]
        date = strip_tags(cell("date"))
        out.append(item(role, url, loc, date if re.search(r"\d{4}", date) else None, "age/date: " + date, company or "(see link)", src_name))
    return out


def run_github(entries, skip, quick):
    stats = {}
    out = []
    for g in entries:
        name = g["name"]
        if skip:
            stats[name] = {"status": "skipped", "n": 0}
            continue
        if quick and g.get("large"):
            stats[name] = {"status": "skipped_quick", "n": 0}
            continue
        t0 = time.time()
        st, txt = http(g["url"], timeout=90)
        if st is None or st >= 400:
            stats[name] = {"status": "error: HTTP %s" % st, "n": 0}
            continue
        items = []
        if g.get("format") == "simplify_json":
            try:
                data = json.loads(txt)
            except Exception:
                stats[name] = {"status": "error: bad json", "n": 0}
                continue
            cutoff = TODAY - dt.timedelta(days=180)
            for r in data:
                if not isinstance(r, dict):
                    continue
                if r.get("active") is False or r.get("is_visible") is False:
                    continue
                locs = r.get("locations") or []
                locs_s = "; ".join(str(x) for x in locs)
                if not re.search(r"canada|ontario|toronto|ottawa|waterloo|kitchener|montr|quebec|qu[ée]bec", locs_s, re.I):
                    continue
                if str(r.get("sponsorship", "")).lower().startswith("u.s. citizenship"):
                    continue
                ds = [x for x in (norm_date(r.get("date_posted")), norm_date(r.get("date_updated"))) if x]
                d = max(ds) if ds else None
                if d and dt.date.fromisoformat(d) < cutoff:
                    continue
                items.append(item(r.get("title"), r.get("url"), locs_s, d, "%s %s" % (r.get("category", ""), r.get("sponsorship", "")), r.get("company_name", ""), "github:" + name))
        else:
            items = parse_md_table(txt, "github:" + name)
        out.extend(items)
        stats[name] = {"status": "ok" if items else "empty", "n": len(items), "secs": round(time.time() - t0, 1)}
    return out, stats


# ----------------------------------------------------------------------------
# Watch pages
# ----------------------------------------------------------------------------

def run_watch(pages, patterns):
    res = []
    for p in pages:
        st, txt = http(p["url"])
        if st is None or st >= 400:
            res.append({"name": p["name"], "url": p["url"], "status": "fetch_failed", "detail": "HTTP %s" % st})
            continue
        vis = strip_tags(txt, 200000)
        closed = [pat for pat in patterns if pat.lower() in vis.lower()]
        links = [x for x in html_generic(txt, p["url"], p["name"]) if "/job" in x["url"].lower() or JOBISH_RE.search(x["title"])]
        digest = hashlib.sha1(vis[:20000].encode("utf-8")).hexdigest()[:12]
        res.append({"name": p["name"], "url": p["url"], "status": "no_openings" if closed and not links else ("has_openings" if links else "unclear"),
                    "matched_phrases": closed[:3], "job_links": len(links), "sample_titles": [x["title"] for x in links[:5]], "digest": digest})
    return res


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", default="sources.json")
    ap.add_argument("--seen", default=None)
    ap.add_argument("--out", default="candidates.json")
    ap.add_argument("--stats", default="stats.json")
    ap.add_argument("--skip", default="", help="comma list: employers,linkedin,eluta,jobbank,vector,getro,hn,ros,github,watch")
    ap.add_argument("--only", default="", help="comma list of employer names to run (substring match)")
    ap.add_argument("--quick", action="store_true", help="skip large downloads")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--max-per-source", type=int, default=300)
    ap.add_argument("--max-per-company", type=int, default=40)
    ap.add_argument("--cap", type=int, default=500)
    args = ap.parse_args()
    skip = set(x.strip() for x in args.skip.split(",") if x.strip())
    t_start = time.time()

    with open(args.sources, encoding="utf-8") as f:
        cfg = json.load(f)
    seen = {}
    if args.seen:
        try:
            with open(args.seen, encoding="utf-8") as f:
                seen = (json.load(f) or {}).get("seen", {}) or {}
        except Exception as e:
            print("WARN: could not read seen file: %s" % e, file=sys.stderr)

    stats = {"run_date": TODAY.isoformat(), "employers": {}, "aggregators": {}, "github": {}, "watch": [], "filters": {}}
    raw = []

    # employers (threaded)
    employers = cfg.get("employers", [])
    if args.only:
        keys = [k.strip().lower() for k in args.only.split(",") if k.strip()]
        employers = [e for e in employers if any(k in e["name"].lower() for k in keys)]
    if "employers" not in skip:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(run_employer, e): e for e in employers}
            for fut in as_completed(futs):
                name, items, st = fut.result()
                stats["employers"][name] = st
                raw.extend(items[: args.max_per_source])
    else:
        stats["employers"]["_all"] = {"status": "skipped"}

    # aggregators: threaded for the polite ones, sequential for the paced ones
    agg = cfg.get("aggregators", {})
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {
            "eluta": ex.submit(run_eluta, agg.get("eluta", {}), "eluta" in skip),
            "vector": ex.submit(run_vector, agg.get("vector", {}), "vector" in skip),
            "getro": ex.submit(run_getro, agg.get("getro", {}), "getro" in skip),
            "hn": ex.submit(run_hn, agg.get("hn_algolia", {}), "hn" in skip),
            "ros": ex.submit(run_ros, agg.get("ros_discourse", {}), "ros" in skip),
            "github": ex.submit(run_github, agg.get("github", []), "github" in skip, args.quick),
        }
        for k, fut in futs.items():
            try:
                items, st = fut.result()
            except Exception as e:
                items, st = [], {"status": "error: %s" % e, "n": 0}
            if k == "github":
                stats["github"] = st
            else:
                stats["aggregators"][k] = st
            raw.extend(items)
    for k, fn, key in (("linkedin", run_linkedin, "linkedin"), ("jobbank", run_jobbank, "jobbank")):
        try:
            items, st = fn(agg.get(key, {}), k in skip)
        except Exception as e:
            items, st = [], {"status": "error: %s" % e, "n": 0}
        stats["aggregators"][k] = st
        raw.extend(items)

    # watch pages
    if "watch" not in skip:
        stats["watch"] = run_watch(cfg.get("watch_pages", []), cfg.get("no_openings_patterns", []))

    # filter + dedupe + seen
    counts = {"raw": len(raw), "kept": 0, "seen": 0, "dup": 0}
    reasons = {}
    keep = {}
    for it in raw:
        ok, reason, flags = classify(it, it.get("employer_source", False))
        if not ok:
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        key = seen_key(it)
        if key in seen:
            counts["seen"] += 1
            continue
        if key in keep:
            counts["dup"] += 1
            # merge sources
            keep[key]["sources"] = sorted(set(keep[key].get("sources", [keep[key]["source"]]) + [it["source"]]))
            continue
        it["key"] = key
        it["flags"] = flags
        it["sources"] = [it["source"]]
        keep[key] = it
    counts["kept"] = len(keep)
    stats["filters"] = {"counts": counts, "dropped_by_reason": reasons}

    cands = list(keep.values())
    # entry-level wording first, then newest; then cap per company so one big board cannot flood the list
    cands.sort(key=lambda x: ("entry_wording" in x.get("flags", []), x.get("posted") or "0000-00-00"), reverse=True)
    per_company = {}
    balanced = []
    for x in cands:
        c = (x.get("company") or "").lower()
        per_company[c] = per_company.get(c, 0) + 1
        if per_company[c] <= args.max_per_company:
            balanced.append(x)
    stats["filters"]["company_capped"] = len(cands) - len(balanced)
    cands = balanced
    if len(cands) > args.cap:
        stats["filters"]["capped_from"] = len(cands)
        cands = cands[: args.cap]
    stats["elapsed_secs"] = round(time.time() - t_start, 1)
    stats["candidates_written"] = len(cands)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(cands, f, ensure_ascii=False, indent=1)
    with open(args.stats, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=1)
    ok_emp = sum(1 for v in stats["employers"].values() if v.get("status") == "ok")
    print("done: %d raw -> %d candidates; employers ok %d/%d; elapsed %ss" % (len(raw), len(cands), ok_emp, len(stats["employers"]), stats["elapsed_secs"]))


if __name__ == "__main__":
    main()
