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
import binascii
import datetime as dt
import email.utils
import gzip
import hashlib
import html
import http.cookiejar as _cookiejar
import json
import re
import sys
import threading
import time
import unicodedata
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

# Per-host gate: at most `concurrency` requests in flight and at least `min_interval`
# seconds between request starts. Several employers share one ATS host (Workable,
# Greenhouse, Lever...), and parallel workers used to trip their rate limits (HTTP 429).
HOST_LIMITS = {"default": {"concurrency": 2, "min_interval": 0.0},
               "workday:wd1": {"concurrency": 6, "min_interval": 0.0}, "workday:wd3": {"concurrency": 6, "min_interval": 0.0},
               "workday:wd5": {"concurrency": 6, "min_interval": 0.0}, "oraclecloud": {"concurrency": 2, "min_interval": 0.0},
               "apply.workable.com": {"concurrency": 1, "min_interval": 2.0},
               "apply.careers.microsoft.com": {"concurrency": 1, "min_interval": 1.5},
               "careers.amd.com": {"concurrency": 1, "min_interval": 5.0}}
_GATES = {}
_GATES_LOCK = threading.Lock()


class _HostGate:
    def __init__(self, concurrency, min_interval):
        self.sem = threading.Semaphore(max(1, int(concurrency)))
        self.min_interval = float(min_interval)
        self.lock = threading.Lock()
        self.last = 0.0

    def __enter__(self):
        self.sem.acquire()
        with self.lock:
            wait = self.last + self.min_interval - time.time()
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
        return self

    def __exit__(self, *exc):
        self.sem.release()


def _gate(url):
    host = urllib.parse.urlsplit(url).netloc.lower()
    # tenants of one Workday data centre (wd1, wd3, wd5...) share an IP, so they share one gate
    m = re.search(r"\b(wd\d+)\.myworkday(?:jobs|site)\.com$", host)
    key = ("workday:" + m.group(1)) if m else ("oraclecloud" if host.endswith(".oraclecloud.com") else host)
    with _GATES_LOCK:
        g = _GATES.get(key)
        if g is None:
            lim = HOST_LIMITS.get(key) or HOST_LIMITS.get(host) or HOST_LIMITS["default"]
            g = _HostGate(lim.get("concurrency", 3), lim.get("min_interval", 0.0))
            _GATES[key] = g
        return g


def http(url, method="GET", body=None, headers=None, timeout=25, retry_429=True):
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
    retry_after = None
    with _gate(url):
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
            if status == 429 and e.headers:
                retry_after = e.headers.get("Retry-After")
        except Exception as e:  # timeout, DNS, TLS...
            if "SSL" in str(e) or "handshake" in str(e).lower():
                return _curl(url, method, data, hdrs, timeout)
            return None, "%s: %s" % (type(e).__name__, e)
    if status == 429 and retry_429:
        try:
            wait = min(60.0, max(5.0, float(retry_after)))
        except (TypeError, ValueError):
            wait = 20.0
        time.sleep(wait)
        return http(url, method, body, headers, timeout, retry_429=False)
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
    r"Saint-Hubert|Bromont|Trois-Rivi[èe]res|Saguenay|Saint-Eustache|Sainte-Claire|Mont-Royal|Mount-Royal|Gentilly|"
    r"St\.? Thomas|Lindsay|Belleville|Cobourg|Brockville|Cornwall|Sarnia|Stratford|St\.? Catharines|Welland|Thunder Bay|"
    r"Timmins|Sault Ste\.? Marie|Orillia|Bolton|Caledon|Stoney Creek|Ancaster|Thornhill|Uxbridge|Smiths Falls|Renfrew|"
    r"Petawawa|Deep River|Pembroke|Orl[ée]ans|Gloucester|Barrhaven|Stittsville|Garson|Val-d'Or|"
    r"Saint-J[ée]r[ôo]me|Saint-Jean-sur-Richelieu|Terrebonne|Repentigny|Granby|Drummondville|Victoriaville|"
    r"Saint-Hyacinthe|Joliette|Rimouski|Rivi[èe]re-du-Loup|B[ée]cancour|Shawinigan|Sorel-Tracy|Valleyfield|Vaudreuil|"
    r"Ch[âa]teauguay|Candiac|La Prairie|Saint-Lambert|Sainte-Th[ée]r[èe]se|Mascouche|Lachute|Rouyn-Noranda|Sept-[ÎI]les|"
    r"Baie-Comeau|Chicoutimi|Jonqui[èe]re|Thetford|Magog|Cowansville|Beauport|Sainte-Foy|Beaconsfield|Anjou|Verdun|"
    r"LaSalle|Westmount|Saint-Georges|Sainte-Marie|Mont-Saint-Hilaire|Beloeil|Chambly|Sainte-Julie|Laurentides|Mont[ée]r[ée]gie)",
    re.I)
OUTSIDE_RE = re.compile(
    r"\b(BC|British Columbia|Vancouver|Victoria|Burnaby|Richmond, BC|Alberta|Calgary|Edmonton|Manitoba|Winnipeg|"
    r"Saskatchewan|Saskatoon|Regina|Nova Scotia|Halifax|Dartmouth|New Brunswick|Fredericton|Moncton|Newfoundland|"
    r"St\.? John'?s|PEI|Prince Edward|Yukon|Nunavut|Northwest Territories|United States|USA|U\.S\.|California|Texas|"
    r"Washington|New York|Massachusetts|Michigan|Arizona|Colorado|Florida|Georgia|Illinois|Pennsylvania|Virginia|"
    r"North Carolina|Ohio|Oregon|Nevada|Utah|Minnesota|Wisconsin|Maryland|Bethesda|Iowa|Vermont|Connecticut|New Jersey|"
    r"New Hampshire|Maine|Rhode Island|Delaware|Kentucky|Tennessee|Alabama|Mississippi|Louisiana|Arkansas|Missouri|"
    r"Kansas|Nebraska|Oklahoma|South Carolina|West Virginia|Indiana|Idaho|Montana|Wyoming|Dakota|New Mexico|Hawaii|Alaska|"
    r"India|Bangalore|Bengaluru|Hyderabad|"
    r"Pune|Chennai|Germany|N[uü]rnberg|France|Paris|United Kingdom|England|Israel|Poland|Sweden|Finland|Japan|Korea|"
    r"Seoul|China|Shanghai|Beijing|Shenzhen|Singapore|Australia|Mexico|Brazil|Netherlands|Ireland|Dublin|Spain|Italy|"
    r"Switzerland|Belgium|Romania|Czech|Hungary|Portugal|Taiwan|Vietnam|Philippines|Malaysia)\b|\bUS-[A-Z]{2}\b|^(GB|US|DE|FR|IN|MX|AU|PL|SE|CZ)\.|, ?(US|USA|MX|AU|GB|UK|DE|FR|IN|CN|JP|KR|SG|BR|IE|NL|PL|IL|SE|FI|ES|IT|CH|BE|RO|CZ|HU|PT|TW|VN|PH|MY|AE|SA)\b|\b[A-Z]{2}, (US|USA)\b", re.I)
OUTSIDE_CS_RE = re.compile(r"(?<![A-Za-z])(US|USA|U\.S\.A?\.?|UK)(?![A-Za-z])|,\s*(AL|AK|AZ|AR|CO|CT|DE|FL|GA|HI|ID|IL|"
                           r"IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|"
                           r"VT|VA|WA|WV|WI|WY)\b")
SENIOR_RE = re.compile(
    r"\b(senior|s[ée]nior(e)?|sr\.?|staff|principal|principle|distinguished|fellow|lead|leader|head of|manager|gestionnaire|director|directeur|"
    r"avp|specialist|sp[ée]cialiste|product owner|"
    r"experienced|exp[ée]riment[ée]e?|"
    r"directrice|vp|vice president|architect|architecte|intermediate|interm[ée]diaire|mid[- ]level|mid[- ]senior|"
    r"expert|supervisor|superviseur|chef d'[ée]quipe|iii|iv|level [3-9]|l[4-9]|niveau [3-9]|10\+|[5-9]\+ ?(years|yrs|ans))\b",
    re.I)
NONENG_RE = re.compile(
    r"\b(sales|account executive|account manager|marketing|recruit|talent acquisition|human resources|payroll|"
    r"accountant|accounting|comptable|legal|counsel|paralegal|nurse|driver|chauffeur|warehouse|janitor|custodian|"
    r"customer success|financial controller|contr[ôo]leur financier|"
    r"security guard|receptionist|administrative|customer service|buyer|acheteur|procurement|purchasing|inventory|"
    r"assembler|assembleur|welder|soudeur|machinist|machiniste|forklift|dispatcher|cashier|cook|electrician|"
    r"plumber|millwright|articling|communications|brand|social media|event|treasury|tax|audit|actuar|underwrit|"
    r"claims|mortgage|banking advisor|teller|physician|pharmac|dental|clinical|therapist|professor|faculty|lecturer|postdoc|clerk|coordinator|coordonn|planner|planificateur|scheduler|recruiter|actuar|consultant|proposal|administrator|admin|venture|fund|orthop|registered|licensed practical|caregiver|teacher|tutor|librarian|chaplain|barista|server|host|guard|cleaner|labou?rer|painter|carpenter|mechanic\b|installer|field service|technicien|technician(?! engineer))\b", re.I)
SALES_RECRUIT_RE = re.compile(r"\b(sales|recruit\w*|talent acquisition|account (executive|manager)|marketing|"
                              r"business development)\b", re.I)
HARD_NONENG_RE = re.compile(
    r"technician|technicien|technologist|technologue|assembler|assembleur|operator|op[ée]rat(eur|rice)|repair|r[ée]parat|"
    r"installer|installateur|welder|soudeur|machinist|machiniste|mechanic\b|m[ée]canicien|electrician|[ée]lectricien|"
    r"millwright|\bapprentic(e|es|eship)\b|\bapprenti(e|es|s)?\b|\binspect(or|eur|rice)\b|adjoint(e)? technique|driver|chauffeur|annotat|labell?er|rater\b|speaker|tutor|translator|writer\b|faculty|professor|"
    r"banker|personal banking|teller|coordinator|coordonnat|instructional|technical sales|"
    r"lecturer|tenure|teaching assistant|\bTA\b|\bCUPE\b|charg[ée]e? de cours|post-?doc|"
    r"\b(robotics|mech|maintenance|field|lab|electronics?|controls?|automation|plc|service|services|it|cnc) tech\b|\btech\s*$", re.I)
INTERN_RE = re.compile(
    r"\b(intern|internship|co-?op|stagiaire|stage|student|[ée]tudiant(e)?|summer 20\d\d|winter 20\d\d|fall 20\d\d|"
    r"spring 20\d\d|\bPEY\b|professional engineering year|work term|placement)\b", re.I)
NEWGRAD_RE = re.compile(
    r"(new[- ]grad|new graduate|nouveaux? dipl[ôo]m|recent graduate|r[ée]cent(e)?s? dipl[ôo]m|graduating|class of 2027|"
    r"\b2027\b|entry[- ]level|premier [ée]chelon|junior|\bjr\.?\b|associate|engineer i\b|engineer 1\b|developer i\b|"
    r"\bdipl[ôo]m[ée]|programme d'acc[ée]l[ée]ration|"
    r"scientist i\b|level 1\b|\bL1\b|\bEIT\b|engineer[- ]in[- ]training|ing[ée]nieur(e)? stagiaire|\bCPI\b|"
    r"graduate (engineer|program|programme|development|trainee|rotation|scheme)|rotational|leadership development|"
    r"early[- ]career|early talent|campus|university grad|university recruiting|trainee|d[ée]butant|"
    r"nouvel(le)? dipl|0-[23] years|0 to [23] years)", re.I)
DOMAIN_RE = re.compile(
    r"(robot|autonom|self[- ]driving|driverless|perception|computer vision|\bvision\b|machine learning|\bML\b|\bAI\b|"
    r"artificial intelligence|deep learning|reinforcement|\bcontrol|\bGNC\b|guidance|navigation|motion|planning|"
    r"embedded|firmware|\bFPGA\b|\bDSP\b|mechatronic|m[ée]catronique|systems? engineer|ing[ée]nieur(e)? syst|"
    r"test engineer|validation|verification|v[ée]rification|simulation|hardware|electrical|[ée]lectrique|electronic|"
    r"[ée]lectronique|signal processing|sensor|capteur|lidar|radar|camera|\bSLAM\b|localization|mapping|research|"
    r"recherche|scientist|scientifique|data scien|applied scien|software (engineer|developer)|d[ée]veloppeu(r|se)|"
    r"ing[ée]nieur|ing[ée]nierie|g[ée]nie\b|engineer|aerospace|a[ée]rospatial|avionic|propulsion|automation|automatisation|instrumentation|"
    r"\bI&C\b|power system|nuclear|nucl[ée]aire|\brail|signalling|signaling|vehicle|v[ée]hicule|automotive|\bADAS\b|"
    r"\bEV\b|battery|batterie|drone|\bUAV\b|\bspace\b|satellite|quantum|optic|photonic|semiconductor|\bASIC\b|"
    r"\bRTL\b|silicon|compiler|\bGPU\b|\bML\b|analytics|data engineer|technologist|technologue|mechanical|m[ée]canique)",
    re.I)


OFFFIELD_RE = re.compile(
    r"(civil|structural|structure|g[ée]otech|g[ée]olog|ecolog|[ée]cologi|environmental|environnement|hydrogeolog|hydrolog|"
    r"hydrotechn|hydraulique|tuyauterie|proc[ée]d[ée]s|mining|minier|mineral|min[ée]ral|metallurg|m[ée]tallurg|refrigeration|"
    r"r[ée]frig[ée]ration|geoscien|g[ée]oscien|coastal|c[ôo]tier|water treatment|traitement (de l'|des )?eaux?|b[âa]timent|"
    r"building (mechanical|services|systems)|"
    r"HVAC|piping|plumbing|petroleum|chemical process|process engineer|water resources|wastewater|transportation planning|"
    r"traffic|surveying|architectural|building science|\bconstruction|contracts?\b|supply chain|logistics|business analyst|"
    r"\bDBA\b|database administrator|\bSAP\b|salesforce|\bCRM\b|\bERP\b|clearing|actuar|\btax\b|audit|cost controller|"
    r"land development|municipal|bridge|\bponts?\b|ouvrages? d.art|stormwater|eaux pluviales|g[ée]nie routier|\broutier|"
    r"roadway|highway|pavement|GIS analyst|urban|die process|tooling|weld|"
    r"maintenance|facilities|quality inspector|production|manufacturing engineer|industrial engineer|reliability engineer)", re.I)
INSCOPE_RE = re.compile(
    r"(robot|autonom|\bcontrols?\b|embedded|firmware|perception|vision|machine learning|\bML\b|\bAI\b|mechatronic|automation|"
    r"software|electrical|electronic|instrument|simulation|\btest\b|validation|systems? engineer|signal|sensor|aerospace|"
    r"avionic|propulsion|research|scientist|data scien|\bGNC\b|navigation|hardware|FPGA|ASIC|nuclear|I&C|rotation|"
    r"leadership development)", re.I)
NG_STRICT_RE = re.compile(
    r"(new[- ]grad|new graduate|nouveaux? dipl[ôo]m|recent graduate|(?<!under)graduate (program|programme|trainee|rotation|scheme|engineer)|"
    r"rotational|rotation program(me)?|programme de rotation|nouveaux? gradu|early[- ]career|entry[- ]level|\bEIT\b|"
    r"engineer[- ]in[- ]training|leadership development|\bCPI\b|\bOVIN\b|talent development program)", re.I)
EXPLICIT_NG_RE = re.compile(
    r"(new[- ]grad|new graduate|nouveaux? dipl[ôo]m|recent graduate|r[ée]cent(e)?s? dipl[ôo]m|entry[- ]level|"
    r"premier [ée]chelon|\bEIT\b|engineer[- ]in[- ]training|(?<!under)graduate (engineer|program|programme|development|trainee|"
    r"rotation|scheme)|rotational|rotation program(me)?|programme de rotation|leadership development|early[- ]career|"
    r"university grad|junior|\bjr\.?\b|associate|nouveaux? gradu|\bOVIN\b|talent development program|"
    r"\bdipl[ôo]m[ée]\.?e?s?\b|programme d'acc[ée]l[ée]ration)", re.I)
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
    if SALES_RECRUIT_RE.search(t) and not re.search(r"engineer|ing[ée]nieur|developer|scientist", t, re.I):
        return False, "non_engineering", flags
    if (HARD_NONENG_RE.search(t) and not re.search(r"\bengineer\b|\bing[ée]nieur(e)?\b|developer|d[ée]veloppeu|scientist", t, re.I)
            and not NG_STRICT_RE.search(t)):
        return False, "non_engineering", flags
    if SENIOR_RE.search(t):
        return False, "senior", flags
    if (re.search(r"\bII\b", t) or re.search(r"\b(engineer|scientist|developer|analyst|designer)\s+2\b", t, re.I)) \
            and not EXPLICIT_NG_RE.search(t):
        return False, "senior", flags
    if INTERN_RE.search(t) and not NG_STRICT_RE.search(t):
        return False, "internship", flags
    if OFFFIELD_RE.search(t) and not INSCOPE_RE.search(t):
        return False, "off_field", flags
    in_onqc = bool(LOC_RE.search(loc))
    outside = bool(OUTSIDE_RE.search(loc) or OUTSIDE_CS_RE.search(loc))
    if in_onqc and outside and not re.search(r"\b(ON|QC)\b|Ontario|Qu[ée]bec|Canada", loc):
        in_onqc = False   # "Cambridge, Massachusetts", "Waterloo, Iowa": a US town that shares an Ontario name
    if outside and not in_onqc:
        return False, "outside_onqc", flags
    if not in_onqc:
        if employer_source:
            if (loc and not it.get("loc_default") and not it.get("unknown_location_ok")
                    and not re.search(r"canada|remote|locations|hybrid|multiple|various|home", loc, re.I)):
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


def short_key(k):
    """16-hex digest of a seen key (the v2 state format)."""
    if re.fullmatch(r"[0-9a-f]{16}", k or ""):
        return k
    return hashlib.sha1(k.encode("utf-8")).hexdigest()[:16]


def key8(k):
    """8-hex key used by the v3 state: the first 8 hex digits of short_key, so v2 keys convert by truncation."""
    if re.fullmatch(r"[0-9a-f]{8}", k or ""):
        return k
    return short_key(k)[:8]


# ----------------------------------------------------------------------------
# Cross-source identity: the same requisition reached through LinkedIn, Eluta, a
# GitHub list and the employer board has four URLs but one company/title/city.
# ----------------------------------------------------------------------------

CO_STOP = {"inc", "ltd", "limited", "corp", "corporation", "co", "company", "llc", "lp", "ulc", "canada", "of",
           "the", "group", "technologies", "technology", "international", "holdings", "incorporated", "ltee",
           "sa", "ag", "gmbh", "plc", "canadian", "north", "america", "americas", "na"}
TITLE_NOISE_RE = re.compile(r"\b(remote|hybrid|hybride|on[- ]?site|full[- ]?time|temps plein|permanent|contract|"
                            r"temporary|fte|req(uisition)?|job id|ref)\b|\b(jr|r|req)[- ]?\d{3,}\b|#\s*\d+|\b\d{5,}\b", re.I)


def _ascii(s):
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()


def norm_company(c):
    c = _ascii(re.sub(r"\(.*?\)", " ", (c or "").split("/")[0]))
    toks = [t for t in re.sub(r"[^a-z0-9+&]+", " ", c).split() if t not in CO_STOP]
    return " ".join(toks[:3])


def norm_title(t):
    t = t or ""
    parts = re.split(r"\s+[-–|]\s+", t)
    if len(parts) > 1 and (LOC_RE.search(parts[-1]) or OUTSIDE_RE.search(parts[-1])) and len(parts[-1]) < 45:
        t = " - ".join(parts[:-1])
    t = re.sub(r"\(.*?\)", " ", t)
    t = TITLE_NOISE_RE.sub(" ", _ascii(t))
    return " ".join(re.sub(r"[^a-z0-9+#]+", " ", t).split())


def norm_city(loc):
    m = LOC_RE.search(loc or "")
    if not m:
        return ""
    c = _ascii(m.group(0)).strip(" ,")
    return {"montreal": "montreal", "qc": "quebec", "on": "ontario", ", on": "ontario"}.get(c, c)


def fuzzy_key(it):
    co = norm_company(it.get("company", ""))
    ti = norm_title(it.get("title", ""))
    if not co or not ti:
        return None
    return "fz:%s|%s|%s" % (co, ti, norm_city(it.get("location", "")))


# ----------------------------------------------------------------------------
# Preliminary score: the rubric in routine-prompt.md applied to title, snippet and
# source metadata, so every run ranks the same row the same way. The routine opens
# rows scoring 50+ and adjusts the score after reading the page.
# ----------------------------------------------------------------------------

FAMILY_TITLE_RES = [
    (1, re.compile(r"robot|autonom|self[- ]driving|driverless|motion planning|path planning|\bROS ?2?\b|manipulat|\bAMR\b|"
                   r"humanoid|embodied|mobile robot|robotique", re.I)),
    (2, re.compile(r"perception|computer vision|\bvision\b|machine learning|\bML\b|deep learning|\bAI\b|\bA\.I\.|"
                   r"artificial intelligence|intelligence artificielle|\bIA\b|apprentissage|neural|lidar|sensor fusion|\bSLAM\b|"
                   r"locali[sz]ation|mapping|\b3D\b|image processing|imaging|applied scientist|vision par ordinateur", re.I)),
    (3, re.compile(r"\bcontrols?\b|\bGNC\b|guidance|navigation|motion control|model[- ]based|simulink|flight control|"
                   r"mechatronic|m[ée]catronique|automation|automatisation|commande|asservissement|\bPLC\b|\bAPI\b automat|"
                   r"\bSCADA\b|\bDCS\b|\bHMI\b", re.I)),
    (4, re.compile(r"research|recherche|\bR ?& ?D\b|(?<!data )scientist|scientifique(?! de donn)", re.I)),
    (5, re.compile(r"embedded|firmware|\bFPGA\b|\bBSP\b|device driver|\bRTOS\b|microcontroller|\bMCU\b|hardware|electronic|"
                   r"[ée]lectronique|embarqu|\bASIC\b|\bRTL\b|digital design|silicon|\bSoC\b|\bDSP\b|signal processing|"
                   r"traitement (du|de) signal|\bPCB\b", re.I)),
    (6, re.compile(r"systems? (design |integration |test |verification )?engineer|system design|ing[ée]nieur(e)? (en )?syst[eè]mes?|\btest|\bessais?\b|validation|verification|"
                   r"v[ée]rification|\bV&V\b|integration|int[ée]gration|\bHIL\b|\bSIL\b|simulation|avioni(c|que)|propulsion|"
                   r"aerospace|a[ée]rospatial|nuclear|nucl[ée]aire|\brail|signall?ing|vehicle|v[ée]hicule|automotive|\bADAS\b|"
                   r"power systems?|instrumentation|\bI&C\b|electrical|[ée]lectrique|electrification|battery|batterie|"
                   r"reliability|fiabilit|\bspace\b|spatial|satellite|drone|\bUAV\b|\bUAS\b", re.I)),
    (7, re.compile(r"\bEIT\b|engineer[- ]in[- ]training|\bCPI\b|(?<!under)graduate (program|programme|engineer|trainee|rotation|development|"
                   r"engineering)|rotational|rotation program|leadership development|new grad(uate)? program|early[- ]career program|"
                   r"(new grad|graduate|early[- ]career|campus).{0,40}\bprogram(me)?\b|"
                   r"programme (de )?(rel[eè]ve|dipl[ôo]m)|programme\b.{0,40}(carri[eè]re|acc[ée]l[ée]ration|nouveaux)|"
                   r"\btrainee\b", re.I)),
    (8, re.compile(r"data scien|scientifique de donn|analytics|data analyst|analyste de donn", re.I)),
]
FAMILY_SNIPPET_RE = re.compile(r"robot|autonomous|perception|computer vision|machine learning|deep learning|\bcontrols?\b|"
                               r"embedded|firmware|\bFPGA\b|\bROS\b|simulink|mechatronic|sensor fusion|\bSLAM\b", re.I)
FAMILY_POINTS = {1: 30, 2: 30, 3: 30, 4: 22, 5: 22, 6: 22, 7: 15, 8: 15}
NOTFIT_TITLE_RE = re.compile(
    r"front[- ]?end|full[- ]?stack|back[- ]?end|\bweb\b|devops|dev ops|\bcloud (engineer|architect|developer|operations|"
    r"infrastructure|security|support)|\bSRE\b|site reliability|help ?desk|\bIT (support|specialist|analyst|technician)|"
    r"network (admin|administrator|engineer|analyst)|salesforce|\bSAP\b|business (analyst|intelligence|systems)|"
    r"product (manager|owner|designer|marketing)|project (manager|coordinator|controller)|scrum|\bUX\b|\bUI\b|graphic|"
    r"game (design|designer|programmer)|gameplay|mobile (developer|engineer)|\biOS\b|android|cyber|security (analyst|"
    r"engineer|specialist|architect)|penetration|database|\bDBA\b|technical writer|support (engineer|specialist)|"
    r"solutions? (engineer|architect|consultant)|pre-?sales|sales engineer|customer success|implementation (consultant|"
    r"specialist|engineer)|stationary engineer|building operator|power engineer|estimator|cost engineer|\bconstruction|"
    r"\bcivil\b|structural|geotech|environmental|chemical|process engineer|piping|\bHVAC\b|mechanical design|drafter|"
    r"draftsperson|\bCAD\b|data engineer|analytics engineer|etl|\bBI\b|quality assurance|\bQA\b|tester|manual test|"
    r"business|financial|finance|accounting|marketing|legal|consultant|consulting|conseiller|advisor|analyste d'affaires|"
    r"platform engineer|infrastructure engineer|\brisk\b|credit|fraud|\bAML\b|sanction|underwrit|insurance|pricing|"
    r"customer|commercial|trading|portfolio|speaker|annotat|help improve ai|ai trainer|rater\b|internal control|"
    r"project controls?|cost control|cash control|air traffic|\bATC\b|fire (prevention|protection|safety)|"
    r"aircraft maintenance|maintenance engineer|relations developer|developer relations|devrel|developer advocate", re.I)
RESEARCH_TECH_RE = re.compile(r"engineer|ing[ée]nieur|scien|technical|technolog|\blab|robot|\bAI\b|\bIA\b|\bML\b|data|"
                              r"electr|[ée]lectr|mechan|m[ée]can|physic|comput|software|logiciel|hardware|sensor|imag|system|"
                              r"aero|a[ée]ro|space|spatial|nuclear|energy|[ée]nergie|control|automat|vision|learning|"
                              r"apprentissage|program|optic|photon|quantum|signal|autonom|biomed", re.I)
RESEARCH_LAB_RE = re.compile(r"national research council|\bNRC\b|\bCNRC\b|vector institute|\bmila\b|ivado|\blab", re.I)
GENERIC_SW_RE = re.compile(r"software|logiciel|developer|d[ée]veloppeu|programmer|programmeu|\bSDE\b", re.I)
GENERIC_ENG_RE = re.compile(r"engineer|ing[ée]nieur", re.I)
# engineering titles that stay out of the families even at a robotics or aerospace employer
WEAK_ENG_RE = re.compile(r"mechanical|m[ée]canique|quality|qualit[ée]|manufactur|fabrication|industrial engineer|process|"
                         r"materials?|mat[ée]riaux|packaging|facilit|production|plant|project engineer|estimat|design engineer|"
                         r"drafting|tooling|stress|structures?\b|composite|supplier|sourcing|commodity|cost|maintenance|"
                         r"field (service|engineer)|service engineer|customer|sales|procurement|purchasing|buyer|supply", re.I)
DEGREE_TITLE_RE = re.compile(r"\bph\.?\s?d\b|doctoral|post-?doc|master'?s (student|degree)|\bmsc\b|ma[îi]trise", re.I)
YEARS_LOW_RE = re.compile(r"\b0\s*(?:-|–|to|à)\s*[1-3]\s*(?:\+\s*)?(?:years?|yrs|ans)\b", re.I)
YEARS_MID_RE = re.compile(r"\b[1-3]\s*(?:-|–|to|à)\s*[2-5]\s*(?:\+\s*)?(?:years?|yrs|ans)\b|\b[12]\+?\s*(?:years?|yrs|ans)\b", re.I)
TIMING_RE = re.compile(r"\b2027\b|graduating|class of 20|finissant|upon graduation|expected graduation|"
                       r"(may|june|summer|spring|été|printemps) 2027", re.I)
COMPANY_DOMAIN_RE = re.compile(r"robot|automation|automatisation|autonom|aerospace|a[ée]ro|avionic|controls|mechatronic|"
                               r"vision|motion|drone|\bUAV\b|space|satellite|lidar|sensor|embedded|semiconductor|"
                               r"photonic|quantum|nuclear|rail|defen[cs]e|simulation|silicon", re.I)
PROGRAM_RE = re.compile(r"\bECAP\b|\bEDGE\b|odyssey|\bAI2\b|graduate trainee|new grad(uate)? program|experience@siemens|"
                        r"rotation(al)? program|leadership development|early[- ]career program|graduate program", re.I)
TOOLS_RES = [re.compile(p, re.I) for p in (r"\bROS ?2?\b", r"pytorch", r"matlab", r"simulink", r"\bC\+\+|\bC/C\+\+",
                                           r"embedded", r"perception", r"control (design|systems?|theory)")]
FR_REQ_RE = re.compile(
    r"french (is |language )?(required|essential|mandatory|an? must)|bilingual(ism)? (english[/ -]french |french[/ -]english )?"
    r"(is )?(required|essential|mandatory)|fluen(t|cy) in (both )?(english and french|french)|ma[îi]trise du fran[çc]ais|"
    r"fran[çc]ais (exig|obligatoire|requis|essentiel)|poste bilingue|bilinguisme (exig|requis|essentiel)", re.I)
REMOTE_RE = re.compile(r"hybrid|remote|hybride|t[ée]l[ée]travail", re.I)
SALARY_RE = re.compile(r"\$\s?\d|salary|salaire", re.I)
GOOD_SECTORS = {"robotics", "automotive", "aerospace", "industrial", "research", "medtech", "energy", "rail"}
TARGET_COMPANIES = set()   # normalized employer names from sources.json, filled in main()


HARD_NOTFIT_RE = re.compile(r"internal control|quality control|contr[ôo]le (de la )?qualit[ée]|"
                            r"\b(second|third|fourth) engineer\b|engineering technician|technicien(ne)? en g[ée]nie|project controls?|cost control|cash control|document control|"
                            r"air traffic|\bATC\b|supplier quality|fire (prevention|protection|safety)|aircraft maintenance|"
                            r"relations developer|developer relations|devrel|developer advocate|"
                            r"(financial|project|cost|plant|regional|divisional|area) controller|contr[ôo]leur (financier|de gestion)", re.I)


def family_of(it):
    """Best (lowest-numbered) role family for the row, or None when it is not a fit."""
    t = it.get("title", "") or ""
    snip = it.get("snippet", "") or ""
    if HARD_NOTFIT_RE.search(t):
        return None, "not_fit_title"
    if NOTFIT_TITLE_RE.search(t) and not re.search(r"perception|embedded|firmware|controls? engineer|mechatronic|machine learning|computer vision", t, re.I):
        return None, "not_fit_title"
    if DEGREE_TITLE_RE.search(t):
        return None, "degree_in_title"
    fams = [f for f, rx in FAMILY_TITLE_RES if rx.search(t)]
    if fams == [4] and not (RESEARCH_TECH_RE.search(t) or RESEARCH_LAB_RE.search(it.get("company") or "")):
        return None, "research_without_technical_word"
    if fams:
        return min(fams), "title"
    m = FAMILY_SNIPPET_RE.search(snip)
    if m and (GENERIC_SW_RE.search(t) or GENERIC_ENG_RE.search(t)):
        fams = [f for f, rx in FAMILY_TITLE_RES[:6] if rx.search(m.group(0))]
        if fams:
            return min(fams), "snippet"
    sector = (it.get("sector") or "").lower()
    entry_title = bool(EXPLICIT_NG_RE.search(t) or NEWGRAD_RE.search(t))
    if WEAK_ENG_RE.search(t) and not entry_title:
        return None, "weak_engineering_title"
    if GENERIC_SW_RE.search(t) or GENERIC_ENG_RE.search(t):
        if sector in GOOD_SECTORS:
            return 6, "generic_in_engineering_sector"
        if COMPANY_DOMAIN_RE.search(it.get("company") or ""):
            return 6, "generic_at_engineering_company"
        co = norm_company(it.get("company", ""))
        if co and co in TARGET_COMPANIES and entry_title:
            return 6, "entry_generic_at_target_employer"
        if entry_title and GENERIC_ENG_RE.search(t) and not GENERIC_SW_RE.search(t):
            return 6, "entry_generic_engineering"
        return None, "generic_software_or_engineering"
    return None, "no_family"


# Role type for the digest's Type column (2026-10-12), so engineering roles rank above pure coding roles:
# software = mostly writing code (developer, software, ML/AI/data roles); mixed = engineering with
# substantial code on real hardware (robotics or embedded software, firmware, perception, software verification);
# engineering = hardware, controls, systems, test, I&C, electrical/mechanical, graduate programs, research.
SW_TYPE_RE = re.compile(r"software|logiciel|developer|d[ée]veloppeu|programm|full[- ]?stack|back[- ]?end|front[- ]?end|"
                        r"\bweb\b|devops|\bcloud\b|data (engineer|scien|analyst)|machine learning|\bML\b|\bAI\b|\bIA\b|"
                        r"\bLLM|\bagent|applied scientist|\bQA\b|test automation|compiler|kernel|\bSDET\b|site reliability", re.I)
MIXED_SW_RE = re.compile(r"robot|autonom|perception|embedded|firmware|controls\b|control systems?|motion control|"
                         r"verification|simulation|\bGNC\b|mechatronic|physical ai|\bvol\b|satellite|spatial|embarqu|"
                         r"temps r[ée]el|test (station|bench|rig)|banc d.essai|"
                         r"hardware|sensor|lidar|camera|avionic|flight|vehicle|real[- ]time|\bRTOS\b|FPGA", re.I)
MIXED_ENG_RE = re.compile(r"firmware|perception|autonom|computer vision|machine vision|simulation", re.I)


def role_type(it):
    t = it.get("title", "") or ""
    if SW_TYPE_RE.search(t):
        return "mixed" if MIXED_SW_RE.search(t) else "software"
    return "mixed" if MIXED_ENG_RE.search(t) else "engineering"


def prelim_score(it, source_names=()):
    fam, why = family_of(it)
    t = it.get("title", "") or ""
    snip = it.get("snippet", "") or ""
    text = t + " " + snip
    rtype = role_type(it)
    res = {"family": fam, "family_basis": why, "role_type": rtype}
    if fam is None:
        res.update(prelim=0, parts={})
        return res
    # Before the page is read, an unstated level is unknown (5). The rubric's 15 for "no level stated but a
    # bachelor's degree required" is applied after opening the page, when the requirement is visible.
    # New-grad GitHub lists only carry entry-level postings, so their rows count as explicit entry wording.
    if (EXPLICIT_NG_RE.search(t) or NEWGRAD_RE.search(t) or EXPLICIT_NG_RE.search(snip) or YEARS_LOW_RE.search(snip)
            or any(str(s).startswith("github:") and "2026" not in str(s) for s in source_names)):
        level = 30
    elif YEARS_MID_RE.search(snip):
        level = 0
    else:
        level = 5
    domain = FAMILY_POINTS[fam]
    timing = 15 if (TIMING_RE.search(text) or any("2027" in s for s in source_names)) else 5
    co = norm_company(it.get("company", ""))
    employer = 15 if (it.get("employer_source") or (co and co in TARGET_COMPANIES) or PROGRAM_RE.search(t)) else 5
    extras = 0
    if REMOTE_RE.search((it.get("location") or "") + " " + snip):
        extras += 2
    extras += min(5, sum(1 for rx in TOOLS_RES if rx.search(text)))
    if SALARY_RE.search(snip):
        extras += 3
    extras = min(10, extras)
    french = -10 if FR_REQ_RE.search(text) else 0
    if re.search(r"\b2026\b", t) and re.search(r"grad|campus|dipl|program", t, re.I) and "2027" not in t:
        french -= 10   # 2026 cohort
    software = -10 if rtype == "software" else 0
    total = max(0, min(100, level + domain + timing + employer + extras + french + software))
    res.update(prelim=total, parts={"level": level, "domain": domain, "timing": timing, "employer": employer,
                                    "extras": extras, "french": french, "software": software})
    return res


def load_seen(path):
    """Read the seen state into {8-hex key: date}. Accepts v1 JSON {"seen": {url: date}}, v2 lines
    ('<16-hex key> <date>') and v3 lines ('<date> <8-hex> <8-hex> ...'). The Drive connector may add
    Markdown escapes; malformed lines are skipped so one typo cannot invalidate the whole state."""
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except Exception as e:
        print("WARN: could not read seen file: %s" % e, file=sys.stderr)
        return {}, "missing"
    t = text.strip()
    # Undo every backslash escape that is not a legal JSON escape; hex keys and dates are unaffected.
    t = re.sub(r"\\([^\"\\/bfnrtu])", r"\1", t)
    if t.startswith("{"):
        try:
            raw = ((json.loads(t) or {}).get("seen", {}) or {})
            return {key8(k): d for k, d in raw.items() if isinstance(d, str)}, "json"
        except Exception as e:
            print("WARN: seen JSON unreadable (%s); trying the line format" % e, file=sys.stderr)
    seen, fmt = {}, "lines"
    date_re = re.compile(r"\d{4}-\d{2}-\d{2}$")
    for line in t.splitlines():
        parts = line.strip().lstrip("\\").split()
        if len(parts) < 2:
            continue
        if parts[0].startswith("jobradar-seen"):
            if len(parts) > 1 and parts[1] == "v3":
                fmt = "v3"
            continue
        if date_re.match(parts[0]):
            for k in parts[1:]:
                if re.fullmatch(r"[0-9a-f]{8}", k):
                    seen[k] = parts[0]
            continue
        k, d = parts[0], parts[1]
        if date_re.match(d) and (re.fullmatch(r"[0-9a-f]{16}", k) or k.startswith("http") or k.startswith("h:")):
            seen[key8(k)] = d
    return seen, fmt


def write_seen(path, entries):
    """v3 state: header, then one line per date with up to 16 keys. About 9 bytes per key."""
    by_date = {}
    for k, d in entries.items():
        by_date.setdefault(d, []).append(k)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write("jobradar-seen v3 %s %d\n" % (TODAY.isoformat(), len(entries)))
        for d in sorted(by_date):
            ks = sorted(by_date[d])
            for i in range(0, len(ks), 16):
                f.write("%s %s\n" % (d, " ".join(ks[i:i + 16])))


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
        parts = [str(v.get(k)) for k in ("name", "label", "city", "region", "state", "province", "country", "locationName", "displayName") if v.get(k)]
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
ROLE_NOUN_RE = re.compile(r"(engineer|ing[ée]nieur|developer|d[ée]veloppeu|scientist|scientifique|analyst|analyste|"
                          r"technolog|specialist|sp[ée]cialiste|intern\b|internship|co-?op|stagiaire|graduate|dipl[ôo]m|"
                          r"associate|designer|concepteur|programmer|programmeu|architect|manager|technician|technicien|"
                          r"\bEIT\b|\bCPI\b|trainee|researcher|chercheu|coordinator|conseill|advisor|lead\b|officer)", re.I)
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
        if must_jobish and not ROLE_NOUN_RE.search(inner):
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


def _wd_canada_facets(facets):
    """appliedFacets that restrict a Workday search to Canada: the country facet when the board has one,
    else the location values that name Ontario, Quebec or Canada. (None, how) when neither exists."""
    country, locs = None, []

    def walk(fs):
        nonlocal country
        for f in fs or []:
            if not isinstance(f, dict):
                continue
            param = f.get("facetParameter")
            vals = [v for v in f.get("values") or [] if isinstance(v, dict)]
            for v in vals:
                if v.get("facetParameter"):
                    continue
                d = str(v.get("descriptor") or "").strip()
                if not (param and v.get("id")):
                    continue
                if d.lower() in ("canada", "canada (ca)") and country is None:
                    country = (param, v.get("id"))
                elif re.search(r"\bCanada\b|\b(ON|QC)\b|Ontario|Qu[ée]bec", d) and not OUTSIDE_RE.search(d.replace("Canada", "")):
                    locs.append((param, v.get("id")))
            walk([v for v in vals if v.get("facetParameter")])

    walk(facets)
    if country:
        return {country[0]: [country[1]]}, "country"
    if locs:
        by = {}
        for prm, fid in locs:
            by.setdefault(prm, []).append(fid)
        prm = max(by, key=lambda k: len(by[k]))
        return {prm: by[prm][:100]}, "locations"
    return None, "none"


# Some tenants send no locationsText. Parsons puts 'CA - ON, Markham' or 'CA, NS - Halifax' in the first
# bulletField; Thomson Reuters only has the place in the link (/job/United-States-of-America-Eagan-Minnesota/...).
# Without a location, run_employer would give the row the entry's default (ON/QC) cities.
_WD_BULLET_LOC_RE = re.compile(r"^[A-Z]{2}\s*[-,]\s*[A-Z]{2}\s*[-,]\s*\S")


def workday_location(job):
    loc = job.get("locationsText") or ""
    if loc:
        return loc
    for b in job.get("bulletFields") or []:
        if isinstance(b, str) and _WD_BULLET_LOC_RE.match(b.strip()):
            return b.strip()
    m = re.match(r"/job/([^/]+)/", job.get("externalPath") or "")
    if m:
        place = m.group(1).replace("-", " ").strip()
        if LOC_RE.search(place) or OUTSIDE_RE.search(place):
            return place
    return ""


def p_workday(src):
    url = src["url"]
    m = re.match(r"(https://[^/]+)/wday/cxs/([^/]+)/([^/]+)/jobs", url)
    if not m:
        return [], "bad workday url"
    host, tenant, site = m.group(1), m.group(2), m.group(3)
    base_facets = src.get("body_facets") or {}
    hdr = {"Accept": "application/json"}
    out, errs, got = [], [], set()

    # myworkdaysite.com boards (Magna, Microchip) serve postings under /recruiting/<tenant>/<site>/job/...;
    # host/<site>/job/... answers HTTP 500 there
    if host.endswith(".myworkdaysite.com"):
        link_base = "%s/recruiting/%s/%s" % (host, tenant, site)
    else:
        link_base = "%s/%s" % (host, site)

    def emit(posts):
        for job in posts:
            ext = job.get("externalPath", "")
            u = link_base + ext
            if u in got:
                continue
            got.add(u)
            out.append(item(job.get("title"), u, workday_location(job), job.get("postedOn"),
                            " ".join(job.get("bulletFields", []) or []), "", src["name"],
                            {"detail_api": "%s/wday/cxs/%s/%s%s" % (host, tenant, site, ext)}))

    canada = None
    max_rows = int(src.get("max_rows", 400))
    if not src.get("no_auto_facets"):
        probe, err = get_json(url, "POST", {"appliedFacets": base_facets, "limit": 20, "offset": 0, "searchText": ""}, hdr)
        if probe is None:
            return [], err
        canada, how = _wd_canada_facets(probe.get("facets"))
    if canada:
        applied = dict(base_facets)
        applied.update(canada)
        first, err = get_json(url, "POST", {"appliedFacets": applied, "limit": 20, "offset": 0, "searchText": ""}, hdr)
        if first is not None and int(first.get("total") or 0) > max_rows:
            # a bank-sized Canadian board: read the search terms within Canada rather than every posting
            base_facets, canada = applied, None
    if canada:
        # every Canadian posting, newest pages first as the board orders them (bounded by max_rows)
        total = None
        for off in range(0, max_rows, 20):
            j, err = get_json(url, "POST", {"appliedFacets": applied, "limit": 20, "offset": off, "searchText": ""}, hdr)
            if j is None:
                errs.append("offset %d: %s" % (off, err))
                break
            posts = j.get("jobPostings", []) or []
            if total is None:
                total = int(j.get("total") or 0)
            emit(posts)
            if len(posts) < 20 or off + 20 >= (total or 0):
                break
        return out, ("; ".join(errs) if errs and not out else None)
    # no Canada facet on this board: search terms, up to max_pages pages each (relevance order)
    terms = src.get("search_terms") or ["engineer", "new grad", "junior", "associate", "machine learning", "robotics"]
    for term in terms:
        for off in range(0, 20 * int(src.get("max_pages", 5)), 20):
            body = {"appliedFacets": base_facets, "limit": 20, "offset": off, "searchText": term}
            j, err = get_json(url, "POST", body, hdr)
            if j is None:
                errs.append("%s: %s" % (term, err))
                break
            posts = j.get("jobPostings", []) or []
            emit(posts)
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
    j, err = get_json(src["url"], headers=src.get("headers"))
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
    """RSS/Atom job feed (SuccessFactors, WordPress, Teamtailor, Avature...). A SuccessFactors feed returns only the
    20 newest matches; with keywords_list set, one feed per keyword (full-text match) is read and merged."""
    base = src["url"]
    kws = src.get("keywords_list")   # opt-in: the parser also reads plain RSS feeds (WordPress, Teamtailor, Avature)
    urls = [base]
    if kws:
        stem = re.sub(r"([?&])keywords=[^&]*&?", r"\1", base).rstrip("&?")
        urls = [stem + ("&" if "?" in stem else "?") + "keywords=" + urllib.parse.quote(k) for k in kws]
    out, errs, got = [], [], set()
    for u in urls:
        st, txt = http(u)
        if st is None or st >= 400:
            errs.append("HTTP %s" % st)
            continue
        for it in _rss_items(txt, u, src["name"]):
            if it["url"] not in got:
                got.add(it["url"])
                out.append(it)
    return out, ("; ".join(errs) if errs and not out else None)


SF_ROW_RE = re.compile(r'<a[^>]+class="[^"]*jobTitle-link[^"]*"[^>]+href="([^"]+)"[^>]*>(.*?)</a>(.*?)(?=<a[^>]+class="[^"]*jobTitle-link|</table>|$)', re.I | re.S)


def p_successfactors_html(src):
    out, errs, got = [], [], set()
    pages = int(src.get("max_pages", 6))
    for base in [src["url"]] + list(src.get("extra_urls") or []):
        for start in range(0, 25 * pages, 25):
            u = base + ("&" if "?" in base else "?") + "startrow=%d" % start
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
                ju = abs_url(u, m.group(1))
                if ju in got:
                    continue
                got.add(ju)
                out.append(item(strip_tags(m.group(2)), ju, strip_tags(lm.group(1)) if lm else "",
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


# ----------------------------------------------------------------------------
# More ATS parsers (October 2026 repair round)
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Shared helper: a tiny cookie-keeping session (some boards need a CSRF cookie + header)
# ----------------------------------------------------------------------------

class _Session(object):
    def __init__(self):
        self.jar = _cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.jar))

    def req(self, url, method="GET", body=None, headers=None, timeout=25):
        """Return (status, text); status None on transport failure."""
        import gzip
        hdrs = {"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-CA,en;q=0.9", "Accept-Encoding": "gzip"}
        hdrs.update(headers or {})
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8") if isinstance(body, (dict, list)) else str(body).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/json" if isinstance(body, (dict, list)) else "application/x-www-form-urlencoded")
        rq = urllib.request.Request(url, data=data, method=method, headers=hdrs)
        try:
            with self.opener.open(rq, timeout=timeout) as r:
                raw, st, enc = r.read(), r.status, r.headers.get("Content-Encoding", "")
        except urllib.error.HTTPError as e:
            raw, st, enc = (e.read() or b""), e.code, (e.headers.get("Content-Encoding", "") if e.headers else "")
        except Exception as e:
            return None, "%s: %s" % (type(e).__name__, e)
        if "gzip" in (enc or ""):
            try:
                raw = gzip.decompress(raw)
            except Exception:
                pass
        return st, raw.decode("utf-8", errors="replace")

    def json(self, url, method="GET", body=None, headers=None):
        st, txt = self.req(url, method, body, headers)
        if st is None:
            return None, txt
        if st >= 400:
            return None, "HTTP %s" % st
        try:
            return json.loads(txt), None
        except Exception:
            return None, "not JSON"


# ----------------------------------------------------------------------------
# Dayforce (jobs.dayforcehcm.com, Next.js candidate portal)
# url: https://jobs.dayforcehcm.com/api/geo/<ns>/jobposting/search   body: {"clientNamespace","jobBoardCode",...}
# The site's own front end first GETs /api/auth/csrf (NextAuth) and sends the token as X-CSRF-TOKEN
# together with the __Host-next-auth.csrf-token cookie; without it the POST returns 403 "Forbidden".
# ----------------------------------------------------------------------------

def p_dayforce(src):
    url = src["url"]
    m = re.match(r"(https://[^/]+)/api/geo/([^/]+)/jobposting/search", url)
    if not m:
        return [], "bad dayforce url"
    host, ns = m.group(1), m.group(2)
    body = dict(src.get("body") or {})
    body.setdefault("clientNamespace", ns)
    body.setdefault("jobBoardCode", "CANDIDATEPORTAL")
    body.setdefault("cultureCode", "en-US")
    body.setdefault("distanceUnit", 0)
    board = body["jobBoardCode"]
    s = _Session()
    tok, err = s.json(host + "/api/auth/csrf", headers={"Accept": "application/json"})
    if tok is None or not tok.get("csrfToken"):
        return [], "csrf: %s" % (err or "no token")
    hdrs = {"Accept": "application/json", "X-CSRF-TOKEN": tok["csrfToken"], "Origin": host,
            "Referer": "%s/en-US/%s/%s" % (host, ns, board)}
    out, seen_ids, errs = [], set(), []
    # Quebec boards publish some postings in one language only; "cultures" lists every culture to query.
    for culture in src.get("cultures") or [body["cultureCode"]]:
        body["cultureCode"] = culture
        start = 0
        for _ in range(12):
            body["paginationStart"] = start
            j, err = s.json(url, "POST", body, hdrs)
            if j is None:
                errs.append("%s: %s" % (culture, err))
                break
            posts = j.get("jobPostings") or []
            _dayforce_items(posts, host, ns, board, culture, seen_ids, out, src)
            start += len(posts)
            if not posts or start >= int(j.get("maxCount") or 0):
                break
    return out, ("; ".join(errs) if errs and not out else None)


def _dayforce_items(posts, host, ns, board, culture, seen_ids, out, src):
    for p in posts:
        if p.get("jobPostingId") in seen_ids:
            continue
        seen_ids.add(p.get("jobPostingId"))
        locs = []
        for l in p.get("postingLocations") or []:
            fa = l.get("formattedAddress") or ""
            country = fa.rsplit(",", 1)[-1].strip() if "," in fa else (l.get("isoCountryCode") or "")
            if l.get("cityName"):
                locs.append(", ".join(x for x in (l.get("cityName"), l.get("stateCode"), country) if x))
            elif fa:
                locs.append(fa)
        if p.get("hasVirtualLocation"):
            locs.append("Remote")
        link = "%s/%s/%s/%s/jobs/%s" % (host, culture, ns, board, p.get("jobPostingId"))
        out.append(item(p.get("jobTitle"), link, "; ".join(locs), (p.get("postingStartTimestampUTC") or "")[:10] or None,
                        p.get("jobDescription", "") or "", "", src["name"]))


# ----------------------------------------------------------------------------
# Phenom People career sites (careers.<company>.com/<country>/<lang>/search-results)
# url: https://<host>/widgets (POST); body = the site's own refineSearch request (ddoKey "refineSearch").
# job link: src["job_url_prefix"] + jobSeqNo (verified to resolve to the canonical /job/<id>/<slug> page).
# ----------------------------------------------------------------------------

def p_phenom(src):
    url = src["url"]
    base = dict(src.get("body") or {})
    size = int(base.get("size") or 50)
    prefix = src.get("job_url_prefix") or ("https://%s/global/en/job/" % urllib.parse.urlsplit(url).netloc)
    out, frm, errs = [], 0, []
    for _ in range(int(src.get("max_pages") or 6)):
        body = dict(base, **{"from": frm, "size": size})
        j, err = get_json(url, "POST", body, {"Accept": "application/json", "Content-Type": "application/json"})
        if j is None:
            errs.append(err)
            break
        rs = j.get(base.get("ddoKey") or "refineSearch") or {}
        jobs = ((rs.get("data") or {}).get("jobs")) or []
        for p in jobs:
            loc = p.get("location") or ", ".join(x for x in (p.get("city"), p.get("state"), p.get("country")) if x)
            ml = p.get("multi_location") or []
            if isinstance(ml, list) and len(ml) > 1:
                loc = "; ".join(str(x) for x in ml[:4])
            seq = p.get("jobSeqNo") or p.get("jobId") or p.get("reqId")
            out.append(item(p.get("title"), prefix + str(seq), loc, (p.get("postedDate") or p.get("dateCreated") or "")[:10] or None,
                            p.get("descriptionTeaser", "") or "", "", src["name"]))
        frm += len(jobs)
        if not jobs or frm >= int(rs.get("totalHits") or 0):
            break
    return out, ("; ".join(e for e in errs if e) if errs and not out else None)


# ----------------------------------------------------------------------------
# Taleo Enterprise career sections (<tenant>.taleo.net/careersection/<code>/jobsearch.ftl)
# url: https://<tenant>.taleo.net/careersection/rest/jobboard/searchjobs?lang=en&portal=<portalNo> (POST)
# body: the site's own search JSON; facet ids (e.g. LOCATION Canada) come from the response's facetResults.
# Rows are unkeyed: column[linkedColumn] = title, column[locationsColumns[0]] = JSON list of "CC-Prov-City",
# a column with mm/dd/yyyy = posting date. Detail page: /careersection/<code>/jobdetail.ftl?job=<contestNo>&lang=en
# ----------------------------------------------------------------------------

def p_taleo_rest(src):
    url = src["url"]
    host = "https://" + urllib.parse.urlsplit(url).netloc
    code = src.get("careersection") or "jobs"
    base = dict(src.get("body") or {})
    hdrs = {"Accept": "application/json", "Content-Type": "application/json", "tz": "GMT-04:00"}
    out, errs = [], []
    for page in range(1, int(src.get("max_pages") or 8) + 1):
        body = dict(base, pageNo=page)
        j, err = get_json(url, "POST", body, hdrs)
        if j is None:
            errs.append(err)
            break
        rows = j.get("requisitionList") or []
        for r in rows:
            col = r.get("column") or []
            ti = r.get("linkedColumn") or 0
            title = col[ti] if len(col) > ti else ""
            loc = ""
            for li in r.get("locationsColumns") or []:
                if len(col) > li and col[li]:
                    try:
                        v = json.loads(col[li])
                        v = v if isinstance(v, list) else [str(v)]
                    except Exception:
                        v = [str(col[li])]
                    loc = "; ".join(", ".join(x.split("-", 2)) for x in v)
                    break
            posted = next((c for c in col if isinstance(c, str) and re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", c.strip())), None)
            link = "%s/careersection/%s/jobdetail.ftl?job=%s&lang=en" % (host, code, r.get("contestNo") or r.get("jobId"))
            out.append(item(title, link, loc, posted, "", "", src["name"]))
        pg = j.get("pagingData") or {}
        if not rows or page * int(pg.get("pageSize") or 25) >= int(pg.get("totalCount") or 0):
            break
    return out, ("; ".join(e for e in errs if e) if errs and not out else None)


# ----------------------------------------------------------------------------
# html_jobs: html_generic with a REQUIRED job-link pattern plus card-aware title/location recovery.
#   link_re     regex the absolute href must match (this is what removes nav/product links)
#   title_re    optional regex the title must match
#   extra_urls  further pages (only pages linked from the first one, e.g. Odoo /jobs/page/2)
#   loc_before  also look for a location in the card text before the anchor (Google)
#   next_re     regex whose group 1 is the page's own next-page href (Radancy /location/ pages); followed up to max_pages
# Title order: first <h1-6> inside the anchor (Odoo cards) -> aria-label minus "Learn more about" (Google)
# -> anchor text -> nearest heading before the anchor within the card (HubSpot "LEARN MORE" buttons).
# Relative hrefs resolve against <base href> when the page has one.
# ----------------------------------------------------------------------------

_H_RE = re.compile(r"<h([1-6])\b[^>]*>(.*?)</h\1>", re.I | re.S)
_ARIA_RE = re.compile(r'aria-label="([^"]+)"', re.I)
_BASE_RE = re.compile(r'<base\b[^>]*href="([^"]+)"', re.I)
_ITEMPROP_LOC_RE = re.compile(r'itemprop="(?:addressLocality|addressRegion|addressCountry)"[^>]*>(.*?)<', re.I | re.S)
_TAIL_LOC_RE = re.compile(r"\s[-–|]\s*([^-–|]+)$")


def _loc_window(text):
    m = LOC_RE.search(text) or OUTSIDE_RE.search(text)
    if not m:
        return ""
    pre = re.split(r">|\||  |\. |place ", text[max(0, m.start() - 25): m.start()])[-1]
    post = re.split(r"\||  |\. |bar_chart|Category:|Posted:|Job ID", text[m.end(): m.end() + 40])[0]
    return re.sub(r"</?[A-Za-z][^>]*$", "", pre + text[m.start(): m.end()] + post).strip(" ,;|<")


def p_html_jobs(src):
    link_re = re.compile(src.get("link_re") or r"/jobs?/", re.I)
    title_re = re.compile(src["title_re"], re.I) if src.get("title_re") else None
    urls = [src["url"]] + list(src.get("extra_urls") or [])
    next_re = re.compile(src["next_re"], re.I | re.S) if src.get("next_re") else None
    out, seen, errs, fetched = [], set(), [], set()
    while urls:
        u = urls.pop(0)
        if u in fetched or len(fetched) >= int(src.get("max_pages") or 10):
            continue
        fetched.add(u)
        st, txt = http(u, src.get("method", "GET"), src.get("body"), src.get("headers"))
        if st is None or st >= 400:
            errs.append("HTTP %s" % st)
            continue
        if next_re:  # follow the page's own "next page" link (Radancy: <a class="next" href="...">)
            nm = next_re.search(txt)
            if nm and nm.group(1).strip():
                urls.insert(0, abs_url(u, nm.group(1)))
        bm = _BASE_RE.search(txt)
        base = abs_url(u, bm.group(1)) if bm else u
        prev_end = 0
        for m in A_RE.finditer(txt):
            href = m.group(2)
            if href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue
            full = abs_url(base, href)
            if not link_re.search(full):
                continue
            attrs, inner_html = m.group(1) + " " + m.group(3), m.group(4)
            inner = strip_tags(inner_html, 400)
            seg_before = txt[max(prev_end, m.start() - 3000): m.start()]
            prev_end = m.end()
            title = ""
            hm = _H_RE.search(inner_html)
            if hm:
                title = strip_tags(hm.group(2), 200)
            if not title:
                am = _ARIA_RE.search(attrs)
                if am:
                    title = re.sub(r"^(learn more about|read more about|more about|apply for|apply to|view job:?|view)\s+", "",
                                   strip_tags(am.group(1), 200), flags=re.I)
            if not title and 4 <= len(inner) <= 160 and not (NAV_RE.search(inner) and not JOBISH_RE.search(inner)):
                title = inner
            if not title:
                hs = _H_RE.findall(seg_before[-2500:])
                if hs:
                    title = strip_tags(hs[-1][1], 200)
            if not title or full in seen:
                continue
            loc = ", ".join(x for x in (strip_tags(v) for v in _ITEMPROP_LOC_RE.findall(inner_html)) if x)
            if not loc:
                tm = _TAIL_LOC_RE.search(title)
                if tm and (LOC_RE.search(tm.group(1)) or OUTSIDE_RE.search(tm.group(1))):
                    loc, title = tm.group(1).strip(), title[: tm.start()].strip()
            if not loc:
                loc = _loc_window(inner.replace(title, " ", 1))
            if not loc and src.get("loc_before"):
                loc = _loc_window(strip_tags(re.sub(r"^[^<]*>", "", seg_before)))
            if not loc and not src.get("loc_before"):
                loc = _loc_window(strip_tags(txt[m.end(): m.end() + 600], 300))
            if title_re and not title_re.search(title):
                continue
            seen.add(full)
            out.append(item(title, full, loc, None, "", "", src["name"]))
    return out, ("; ".join(errs) if errs and not out else None)


# ----------------------------------------------------------------------------
# Astro island props (server-rendered JSON inside <astro-island props="...">), e.g. CMC Electronics.
#   url               the search page that carries the island
#   list_key          prop holding the job list (default "jobs")
#   job_url_template  e.g. "https://careers.cmcelectronics.ca/careers{path}/" ({path} = job["path"])
# Astro serialises every value as [type, value]: 0 = plain value/object, 1 = array, 3 = date, ...
# ----------------------------------------------------------------------------

def _astro_decode(v):
    if isinstance(v, list) and len(v) == 2 and isinstance(v[0], int) and not isinstance(v[0], bool):
        tag, val = v
        if tag == 1 and isinstance(val, list):
            return [_astro_decode(x) for x in val]
        if tag == 0 and isinstance(val, dict):
            return {k: _astro_decode(x) for k, x in val.items()}
        return val
    if isinstance(v, dict):
        return {k: _astro_decode(x) for k, x in v.items()}
    return v


def p_astro_jobs(src):
    st, txt = http(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    tmpl = src.get("job_url_template") or (src["url"].rstrip("/") + "{path}")
    key = src.get("list_key") or "jobs"
    out = []
    for pm in re.finditer(r'<astro-island\b[^>]*\sprops="([^"]*)"', txt):
        try:
            props = _astro_decode(json.loads(html.unescape(pm.group(1))))
        except Exception:
            continue
        for job in props.get(key) or [] if isinstance(props, dict) else []:
            if not isinstance(job, dict):
                continue
            c = job.get("context") if isinstance(job.get("context"), dict) else job
            path = job.get("path") or c.get("jobPath") or ""
            parts = [str(c.get(k)).strip() for k in ("city", "state", "country") if c.get(k) not in (None, "", "undefined", "None")]
            loc = ", ".join(parts) or (c.get("location") or "")
            out.append(item(c.get("title"), tmpl.replace("{path}", path), loc, c.get("postingDate"),
                            c.get("description", "") or "", "", src["name"]))
    return out, (None if out else "no astro job props")


# ----------------------------------------------------------------------------
# Workable widget API: one GET returns every published job of an account (no paging).
# url: https://apply.workable.com/api/v1/widget/accounts/<account>   (add ?details=true only if descriptions are needed)
# The v3 endpoint the fetcher uses today pages 10 jobs at a time through a "nextPage" token that p_workable_v3
# never follows (Genetec: 10 of 152 rows read), and each extra page is one more hit on a host that answered 429.
# ----------------------------------------------------------------------------

def p_workable_widget(src):
    j, err = get_json(src["url"], headers={"Accept": "application/json"})
    if j is None:
        return [], err
    account = src["url"].rstrip("/").split("/accounts/")[-1].split("?")[0]
    out = []
    for job in j.get("jobs") or []:
        link = job.get("url") or job.get("shortlink") or ""
        cm = re.search(r"/j/([0-9A-Za-z]+)", link)
        if cm and account:
            link = "https://apply.workable.com/%s/j/%s/" % (account, cm.group(1))
        locs = []
        for l in job.get("locations") or []:
            if isinstance(l, dict):
                locs.append(", ".join(str(l.get(k)) for k in ("city", "region", "country") if l.get(k)))
        if not locs:
            locs = [", ".join(str(job.get(k)) for k in ("city", "state", "country") if job.get(k))]
        if job.get("telecommuting"):
            locs.append("Remote")
        out.append(item(job.get("title"), link, "; ".join(x for x in locs if x),
                        job.get("published_on") or job.get("created_at"),
                        "%s %s %s" % (job.get("department") or "", job.get("employment_type") or "", job.get("experience") or ""),
                        "", src["name"]))
    return out, None


# ----------------------------------------------------------------------------
# Taleo Business Edition career site v2 (tre.tbe.taleo.net/<dc>/ats/careers/v2/searchResults?org=..&cws=..)
# url: the site's own "sort by Refreshed Posting Date, Descending" link (act=sort&sortColumn=3&sortOrder=D).
# Pages hold 10 rows; the "next" link (searchResults?next&rowFrom=N...) only works inside the JSESSIONID
# session that ran the search (stateless "next" returns HTTP 500), so a cookie session follows it.
# Each card: <h4 class="oracletaleocwsv2-head-title"><a href=viewRequisition..>Title</a></h4> then
# <div tabindex="0">location</div><div>employment type</div><div>dd/mm/yyyy</div>.
# ----------------------------------------------------------------------------

_TBE_CARD_RE = re.compile(r'<h4 class="oracletaleocwsv2-head-title">\s*<a href="([^"]+)"[^>]*>(.*?)</a>\s*</h4>(.*?)</div>\s*<!--/\.accordion-head-info', re.S)


def p_taleo_tbe(src):
    import datetime as _dt
    s = _Session()
    host = "https://" + urllib.parse.urlsplit(src["url"]).netloc
    fmt = src.get("date_format") or "%d/%m/%Y"
    pause = float(src.get("pause_seconds") or 0.5)
    st, txt = s.req(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    out, seen = [], set()
    for _ in range(int(src.get("max_pages") or 15)):
        for m in _TBE_CARD_RE.finditer(txt):
            link = m.group(1).replace("&amp;", "&")
            if link in seen:
                continue
            seen.add(link)
            divs = [strip_tags(x) for x in re.findall(r"<div[^>]*>(.*?)</div>", m.group(3), re.S)]
            loc = divs[0] if divs else ""
            posted = None
            for d in divs:
                if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", d):
                    try:
                        posted = _dt.datetime.strptime(d, fmt).date().isoformat()
                    except ValueError:
                        posted = None
            out.append(item(strip_tags(m.group(2)), link, loc, posted, " ".join(divs[1:2]), "", src["name"]))
        nm = re.search(r'href="(/[^"]*searchResults\?next[^"]*)"', txt)
        if not nm:
            break
        time.sleep(pause)
        st, txt = s.req(host + nm.group(1).replace("&amp;", "&"))
        if st is None or st >= 400:
            break
    return out, (None if out else "no rows")


# ----------------------------------------------------------------------------
# SuccessFactors server-rendered lists, several URLs. /search/ URLs page with &startrow=25,50,...;
# /go/<category>/<id>/ pages ignore startrow and page by path (/go/<cat>/<id>/25/?...), so list those
# page URLs explicitly in extra_urls (they are the page's own pagination links). Rows: SF_ROW_RE as in the fetcher.
# ----------------------------------------------------------------------------

def _sf_rows(txt, base, src_name):
    rows = []
    for m in SF_ROW_RE.finditer(txt):
        tail = m.group(3)
        lm = re.search(r'class="[^"]*jobLocation[^"]*"[^>]*>(.*?)</', tail, re.I | re.S)
        dm = re.search(r'class="[^"]*jobDate[^"]*"[^>]*>(.*?)</', tail, re.I | re.S)
        rows.append(item(strip_tags(m.group(2)), abs_url(base, m.group(1)), strip_tags(lm.group(1)) if lm else "",
                         strip_tags(dm.group(1)) if dm else None, "", "", src_name))
    return rows


def p_successfactors_html_multi(src):
    # Page size is set per site (Hatch 25, Kinectrics 21), so the step is the first page's row count and paging
    # stops at the "Results a - b of N" total; the stock parser assumes 25 and stops after page 1 on 21-row sites.
    out, seen, errs = [], set(), []
    for u0 in [src["url"]] + list(src.get("extra_urls") or []):
        start, step, total = 0, None, None
        for _ in range(1 if "/go/" in u0 else int(src.get("max_pages") or 3)):
            u = u0 if "/go/" in u0 else u0 + ("&" if "?" in u0 else "?") + "startrow=%d" % start
            st, txt = http(u)
            if st is None or st >= 400:
                errs.append("HTTP %s" % st)
                break
            rows = _sf_rows(txt, u, src["name"])
            for r in rows:
                if r["url"] not in seen:
                    seen.add(r["url"])
                    out.append(r)
            tm = re.search(r"paginationLabel[^>]*>.*?of\s*<b>\s*([\d,]+)\s*</b>", txt, re.S)
            total = int(tm.group(1).replace(",", "")) if tm else total
            step = step or len(rows)
            start += step or 25
            if not rows or len(rows) < step or (total is not None and start >= total):
                break
    return out, ("; ".join(errs) if errs and not out else None)


# ----------------------------------------------------------------------------
# SuccessFactors sites whose search page is client-rendered (rmk-jobs-search widget: Bombardier, OPG) and whose
# RSS lives under /services/, which every SF robots.txt disallows. Their /sitemap.xml (allowed) is either a
# <urlset> of /job/<City>-<Title>-<Prov>-<Postal>/<reqId>/ links (Bombardier, 1679 links) or an RSS feed.
# This parser reads the urlset form: keeps slugs that start with one of slug_cities, cuts the city prefix and
# the "-ON-L5S-1W1" / "-Québ-H4S-1Y9" tail, and sorts by req id (newest first) so the fetcher's
# --max-per-source cap (300) keeps the newest rows. Titles lose punctuation that the slug dropped.
# ----------------------------------------------------------------------------

def p_sf_sitemap(src):
    st, txt = http(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    cities = src.get("slug_cities") or []
    skip = re.compile(src["skip_title_re"], re.I) if src.get("skip_title_re") else None
    rows = []
    for m in re.finditer(r"<loc>\s*([^<\s]*/job/([^<\s]+?)/(\d+)/?)\s*</loc>", txt):
        url = html.unescape(m.group(1))
        slug = html.unescape(urllib.parse.unquote(m.group(2)))
        city = next((c for c in cities if slug.lower().startswith(c.lower() + "-")), None)
        if cities and not city:
            continue
        rest = slug[len(city) + 1:] if city else slug
        pm = re.search(r"-(ON|QC|Qu[ée]b[a-zé]*|[A-Z]{2})(-[A-Z]\d[A-Z]-?\d[A-Z]\d|-\d{5})?$", rest)
        prov = pm.group(1) if pm else ""
        prov = "QC" if prov.lower().startswith("qu") else prov
        title = (rest[: pm.start()] if pm else rest).replace("-", " ")
        if skip and skip.search(title):
            continue
        loc = ", ".join(x for x in (city, prov) if x)
        rows.append((int(m.group(3)), item(title, url, loc, None, "", "", src["name"])))
    rows.sort(key=lambda x: -x[0])
    out = [r for _, r in rows[: int(src.get("max_rows") or 300)]]
    return out, (None if out else "no matching job urls")


# ----------------------------------------------------------------------------
# json_map: any JSON endpoint whose job list and field names are known (custom WordPress/Next.js APIs where the
# recursive json_generic picks the wrong dicts, e.g. Calian's careerLink {label: "View Job"} and tag dicts).
#   map: {"list": "data.cards", "title": "careerTitle", "url": "careerLink.href", "location": "careerLocation" (or a list of paths),
#         "posted": "careerPostingDate", "snippet": "careerType"}   (dotted paths; list indexes allowed)
#   extra_urls: same request shape for further pages / regions; rows are deduped by url.
# ----------------------------------------------------------------------------

def _dig(o, path):
    for k in [x for x in (path or "").split(".") if x]:
        if isinstance(o, list):
            try:
                o = o[int(k)]
            except (ValueError, IndexError):
                return None
        elif isinstance(o, dict):
            o = o.get(k)
        else:
            return None
    return o


def p_json_map(src):
    m = src.get("map") or {}
    out, seen, errs = [], set(), []
    for u in [src["url"]] + list(src.get("extra_urls") or []):
        j, err = get_json(u, src.get("method", "GET"), src.get("body"), src.get("headers"))
        if j is None:
            errs.append(err)
            continue
        rows = _dig(j, m.get("list")) if m.get("list") else j
        for r in rows if isinstance(rows, list) else []:
            title = _dig(r, m.get("title", "title"))
            link = _dig(r, m["url"]) if m.get("url") else ""
            link = abs_url(u, link) if isinstance(link, str) and link else ""
            if not title or (link and link in seen):
                continue
            seen.add(link)
            snippet = _dig(r, m["snippet"]) if m.get("snippet") else ""
            if isinstance(snippet, list):
                snippet = " ".join(str(x) for x in snippet)
            lp = m.get("location") or []
            loc = ", ".join(x for x in (_loc_from(_dig(r, q)) for q in (lp if isinstance(lp, list) else [lp])) if x and x != "None")
            out.append(item(title, link, loc,
                            _dig(r, m["posted"]) if m.get("posted") else None, snippet or "", "", src["name"]))
    return out, ("; ".join(e for e in errs if e) if errs and not out else None)



# ----------------------------------------------------------------------------
# Apple (jobs.apple.com). Replaces the fetcher's p_apple.
# url: https://jobs.apple.com/api/v1/search (POST, JSON body as the site's own front end sends it; no CSRF token
# needed for search). The response is {"res": {"searchResults": [...], "totalRecords": N}}; the old p_apple read
# searchResults at the top level and therefore always returned 0 rows. 20 rows per page, one row per location
# (a role posted in Toronto and Montreal comes back twice with the same positionId), merged here by positionId.
# Job link: https://jobs.apple.com/en-ca/details/<positionId>/<transformedPostingTitle> (verified HTTP 200).
# ----------------------------------------------------------------------------

def p_apple(src):
    # On 2026-10-12 one call in a parallel run came back {"res":{"searchResults":[],"totalRecords":0}} while 8 of 8
    # sequential calls a minute later returned the 10 rows, so an empty first page is retried once after a pause.
    base = dict(src.get("body") or {})
    page = int(base.get("page") or 1)
    out, by_pid, errs, raw_seen, retried = [], {}, [], 0, False
    for _ in range(int(src.get("max_pages") or 10)):
        j, err = get_json(src["url"], "POST", dict(base, page=page), {"Accept": "application/json"})
        if j is None:
            errs.append(err)
            break
        r = j.get("res", j) if isinstance(j, dict) else {}
        rows = r.get("searchResults") or []
        if not rows and not out and not retried:
            retried = True
            time.sleep(3)
            continue
        raw_seen += len(rows)
        for x in rows:
            pid = str(x.get("positionId") or x.get("id") or "")
            locs = [l.get("name", "") for l in x.get("locations") or [] if isinstance(l, dict) and l.get("name")]
            if pid in by_pid:
                it = by_pid[pid]
                extra = [l for l in locs if l not in it["location"]]
                if extra:
                    it["location"] = strip_tags("; ".join([it["location"]] + extra), 120)
                continue
            slug = x.get("transformedPostingTitle") or ""
            url = "https://jobs.apple.com/en-ca/details/%s%s" % (pid, ("/" + slug) if slug else "")
            team = (x.get("team") or {}).get("teamName", "") if isinstance(x.get("team"), dict) else ""
            it = item(x.get("postingTitle"), url, "; ".join(locs), x.get("postDateInGMT"), team, "", src["name"])
            by_pid[pid] = it
            out.append(it)
        if not rows or raw_seen >= int(r.get("totalRecords") or 0):
            break
        page += 1
    return out, ("; ".join(e for e in errs if e) if errs and not out else None)


# ----------------------------------------------------------------------------
# Eightfold PCSX, paged. Same entry shape as eightfold_pcsx (url + optional extra_urls, each a full
# /api/pcsx/search?... query). Verified 2026-10-12 on lockheedmartin.eightfold.ai, jobs.ericsson.com and
# apply.careers.microsoft.com: every call returns at most 10 positions even with num=50 or num=100, so the stock
# parser keeps only the first 10 per URL. This version walks start=0,10,20,... until data.count is reached.
# ----------------------------------------------------------------------------

def p_eightfold_pcsx_paged(src):
    # apply.careers.microsoft.com answered HTTP 429 to back-to-back calls on 2026-10-12 (even 1 s apart, right after
    # earlier test calls), hence the pause between calls and one slower retry; the retry branch was not exercised live.
    urls = [src["url"]] + list(src.get("extra_urls") or [])
    pace = float(src.get("pace_seconds") or 1.0)
    out, errs, seen, calls = [], [], set(), 0
    for u in urls:
        p = urllib.parse.urlsplit(u)
        q = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
        start = int(dict(q).get("start") or 0)
        q = [(k, v) for k, v in q if k not in ("start", "num")]
        host = "%s://%s" % (p.scheme, p.netloc)
        for _ in range(int(src.get("max_pages") or 15)):
            if calls:
                time.sleep(pace)
            calls += 1
            qs = urllib.parse.urlencode(q + [("start", str(start)), ("num", "10")], quote_via=urllib.parse.quote)
            pu = urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, qs, ""))
            j, err = get_json(pu)
            if j is None and err == "HTTP 429":
                time.sleep(10 * pace)
                j, err = get_json(pu)
            if j is None:
                errs.append(err)
                break
            data = j.get("data") if isinstance(j.get("data"), dict) else j
            positions = data.get("positions") or []
            for ps in positions:
                pid = ps.get("id") or ps.get("positionUrl")
                if pid in seen:
                    continue
                seen.add(pid)
                locs = ps.get("locations") or ps.get("standardizedLocations") or ps.get("location") or ""
                if isinstance(locs, list):
                    locs = "; ".join(str(x) for x in locs)
                purl = ps.get("canonicalPositionUrl") or ps.get("positionUrl") or ""
                out.append(item(ps.get("name"), abs_url(host, purl), locs, ps.get("postedTs") or ps.get("t_update") or ps.get("creationTs"),
                                "%s %s" % (ps.get("department", ""), ps.get("workLocationOption", "")), "", src["name"]))
            start += len(positions)
            if not positions or start >= int(data.get("count") or 0):
                break
    return out, ("; ".join(e for e in errs if e) if errs and not out else None)


# ----------------------------------------------------------------------------
# iCIMS Jibe career sites (<host>/api/jobs?...), the same API icims_json reads, but:
#  - sends Accept-Language en-US: the API filters postings by the request language, and the fetcher's default
#    "en-CA,en;q=0.9,fr;q=0.8" makes careers.amd.com answer totalCount 0 (all AMD postings are tagged en-us);
#    that is why the AMD entry "returned 0 rows" on 2026-09-21. A lang= query parameter does not override it.
#  - limit is capped at 100 (limit=200 returns HTTP 422), so this pages with page=1,2,... until totalCount;
#  - the link is the public job page data.meta_data.canonical_url (e.g. https://careers.amd.com/jobs/91827?lang=en-us,
#    verified 200) instead of the iCIMS login/apply URL.
# ----------------------------------------------------------------------------

def p_jibe(src):
    p = urllib.parse.urlsplit(src["url"])
    q = [(k, v) for k, v in urllib.parse.parse_qsl(p.query, keep_blank_values=True) if k not in ("page", "limit")]
    hdrs = {"Accept": "application/json", "Accept-Language": "en-US,en;q=0.9"}
    hdrs.update(src.get("headers") or {})
    out, errs, seen = [], [], set()
    for page in range(1, int(src.get("max_pages") or 8) + 1):
        qs = urllib.parse.urlencode(q + [("page", str(page)), ("limit", "100")], quote_via=urllib.parse.quote)
        j, err = get_json(urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, qs, "")), headers=hdrs)
        if j is None:
            errs.append(err)
            break
        jobs = j.get("jobs") or []
        for job in jobs:
            d = job.get("data", job) if isinstance(job, dict) else {}
            rid = d.get("req_id") or d.get("slug") or d.get("title")
            if rid in seen:
                continue
            seen.add(rid)
            loc = ", ".join(str(d.get(k)) for k in ("city", "state", "country") if d.get(k))
            md = d.get("meta_data") if isinstance(d.get("meta_data"), dict) else {}
            link = md.get("canonical_url") or d.get("apply_url") or d.get("url") or ""
            cats = d.get("category") or []
            cats = ", ".join(str(c).strip() for c in cats) if isinstance(cats, list) else str(cats)
            out.append(item(d.get("title"), link, loc, d.get("posted_date") or d.get("create_date"),
                            "%s %s" % (cats, strip_tags(d.get("description") or "", 300)), "", src["name"],
                            {"detail_text": strip_tags(d.get("description") or "", 6000)}))
        if not jobs or page * 100 >= int(j.get("totalCount") or 0):
            break
    return out, ("; ".join(e for e in errs if e) if errs and not out else None)


# ----------------------------------------------------------------------------
# ApplicantStack boards (<host>/x/openings): one server-rendered <table id="data-table"> whose header row names the
# columns (TMMC: Title | Employment Group | Section). Keeps every row; a column named like location/city/site
# becomes the location and one named like date/posted the posting date; the rest goes to the snippet.
# html_generic on the same page keeps only anchors whose text passes JOBISH_RE, so a board holding only trades
# rows reads as "empty"; this parser reports them (the prefilter then drops them as non-engineering).
# ----------------------------------------------------------------------------

AS_TABLE_RE = re.compile(r'<table[^>]*id="data-table"[^>]*>(.*?)</table>', re.I | re.S)


def p_applicantstack(src):
    st, txt = http(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    m = AS_TABLE_RE.search(txt or "")
    if not m:
        return [], None
    tbl = m.group(1)
    heads = [strip_tags(h).lower() for h in re.findall(r"<th[^>]*>(.*?)</th>", tbl, re.I | re.S)]
    out = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, re.I | re.S):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.I | re.S)
        a = re.search(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', row, re.I | re.S)
        if not cells or not a:
            continue
        loc, posted, rest = "", None, []
        for i, c in enumerate(cells[1:], 1):
            name = heads[i] if i < len(heads) else ""
            val = strip_tags(c)
            if re.search(r"location|city|site|lieu", name):
                loc = val
            elif re.search(r"date|posted", name):
                posted = val
            elif val:
                rest.append(val)
        out.append(item(a.group(2), abs_url(src["url"], a.group(1)), loc, posted, " | ".join(rest), "", src["name"]))
    return out, None



# ----------------------------------------------------------------------------
# HiBob careers sites (<company>.careers.hibob.com)
# url:  https://<company>.careers.hibob.com/api/job-ad   (GET; header companyidentifier: <company>)
# link: https://<company>.careers.hibob.com/jobs/<id>
#       (Angular route "jobs/:jobAdSlug"; the list component calls router.navigateByUrl(`/jobs/${id}`) and the
#        detail page loads /api/job-ad/<id>/application-form, verified HTTP 200 with the same title)
# ----------------------------------------------------------------------------

def p_hibob(src):
    url = src["url"]
    host = "https://" + urllib.parse.urlsplit(url).netloc
    company = (src.get("headers") or {}).get("companyidentifier") or urllib.parse.urlsplit(url).netloc.split(".")[0]
    hdrs = {"Accept": "application/json", "companyidentifier": company}
    j, err = get_json(url, headers=hdrs)
    if j is None:
        return [], err
    out = []
    for d in j.get("jobAdDetails") or []:
        site, country = d.get("site") or "", d.get("country") or ""
        loc = ", ".join(x for x in (site, country) if x) if site != country else country
        if (d.get("workspaceType") or "").lower() == "remote" and "remote" not in loc.lower():
            loc = (loc + "; Remote").strip("; ")
        snippet = "%s %s %s %s" % (d.get("department") or "", d.get("employmentType") or "", d.get("workspaceType") or "",
                                   strip_tags(d.get("description") or "", 300))
        out.append(item(d.get("title"), "%s/jobs/%s" % (host, d.get("id")), loc, d.get("publishedAt"), snippet, "", src["name"]))
    return out, None


# ----------------------------------------------------------------------------
# Server-rendered job tables/lists on an employer's own page (WordPress, Next.js, Elementor ...)
# src: url, optional method/body/headers (like html_generic), required "row_re" (named groups: title, url,
#      location, posted, snippet; any other named group is appended to the snippet), optional "json_path"
#      (dotted path to an HTML string inside a JSON reply, e.g. WordPress admin-ajax {"data":{"html":...}}),
#      optional "section_re" (one group; its text becomes the location of the rows that follow it),
#      optional "extra_urls" (more pages read the same way) and "unquote" (percent-decode the HTML first,
#      Universite de Sherbrooke).
# ----------------------------------------------------------------------------

def p_html_rows(src):
    row_re = re.compile(src["row_re"], re.I | re.S)
    out, seen, errs = [], set(), []
    for page_url in [src["url"]] + list(src.get("extra_urls") or []):
        st, txt = http(page_url, src.get("method", "GET"), src.get("body"), src.get("headers"))
        if st is None or st >= 400:
            errs.append("HTTP %s" % st)
            continue
        if src.get("json_path"):
            try:
                obj = json.loads(txt)
            except Exception:
                errs.append("not JSON")
                continue
            for k in src["json_path"].split("."):
                obj = obj.get(k) if isinstance(obj, dict) else None
            txt = obj if isinstance(obj, str) else ""
        if src.get("unquote"):
            txt = urllib.parse.unquote(txt)
        base = src.get("base_url") or page_url
        sections = [("", txt)]
        if src.get("section_re"):
            ms = list(re.finditer(src["section_re"], txt, re.I | re.S))
            sections = [(strip_tags(m.group(1)), txt[m.end(): ms[i + 1].start() if i + 1 < len(ms) else len(txt)])
                        for i, m in enumerate(ms)]
        for sec_loc, chunk in sections:
            for m in row_re.finditer(chunk):
                g = m.groupdict()
                title = strip_tags(g.get("title") or "", 200)
                if not title:
                    continue
                u = abs_url(base, g.get("url") or "") or src.get("careers_url", "")
                if (u, title) in seen:
                    continue
                seen.add((u, title))
                extra = " ".join(strip_tags(v) for k, v in g.items()
                                 if k not in ("title", "url", "location", "posted", "snippet") and v)
                loc = strip_tags(g.get("location") or "") or sec_loc
                out.append(item(title, u, loc, strip_tags(g.get("posted") or "") or None,
                                ("%s %s" % (strip_tags(g.get("snippet") or ""), extra)).strip(), "", src["name"]))
    return out, ("; ".join(errs) if errs and not out else None)


# ----------------------------------------------------------------------------
# CommonNinja "Job Listings" widget (cdn.commoninja.com/api/v1/embed/<widget-id>; robots.txt allows /api/)
# Items live at data.widgetData.pluginData.data.content.items; fields are declared in content.customFields
# (the field of type "link" is the Apply link). Items without a link fall back to src["careers_url"].
# ----------------------------------------------------------------------------

def p_commoninja_jobs(src):
    j, err = get_json(src["url"])
    if j is None:
        return [], err
    try:
        content = j["data"]["widgetData"]["pluginData"]["data"]["content"]
    except Exception:
        return [], "no widget content"
    fields = [f for f in content.get("customFields") or [] if isinstance(f, dict)]
    link_ids = [f.get("id") for f in fields if f.get("type") == "link"]
    short_ids = [f.get("id") for f in fields if (f.get("name") or "").lower().startswith("short description")]
    out = []
    for it in content.get("items") or []:
        title = ((it.get("job-title") or {}).get("text") or "").strip()
        if not title:
            continue
        url = next(((it.get(fid) or {}).get("link") for fid in link_ids if (it.get(fid) or {}).get("link")), "")
        short = " ".join((it.get(fid) or {}).get("text") or "" for fid in short_ids)
        desc = (it.get("description") or {}).get("text") or ""
        out.append(item(title, url or src.get("careers_url", ""), "", None, "%s %s" % (short, strip_tags(desc, 300)), "", src["name"]))
    return out, None


# ----------------------------------------------------------------------------
# PeopleSoft Candidate Gateway, Fluid UI (HRS_HRAM_FL.HRS_CG_SEARCH_FL), e.g. SickKids and Western University.
# url: <host>/psc/<db>/<portal>/HRMS/c/HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL?Page=HRS_APP_SCHJOB_FL&Action=U[&SiteId=n]
# One GET in a cookie session returns the whole grid server-rendered (a cookie-less client gets a "cookies
# enabled" page). Values sit in <span class='ps_box-value' id='<FIELD>$<row>'>. Rows carry no links; the posting
# opens at Page=HRS_APP_JBPST_FL with the Job ID. With en-CA PeopleSoft prints dd/mm/yyyy, which norm_date reads
# as mm/dd, so the request asks for en-US (yyyy-mm-dd). Optional "skip_family_re" drops rows by Job Family.
# ----------------------------------------------------------------------------

_PS_CG_FIELD_RE = re.compile(r"id='(SCH_JOB_TITLE|HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID|LOCATION|HRS_APP_JBSCH_I_HRS_DEPT_DESCR|"
                             r"JOB_FAMILY_LABEL|SCH_OPENED)\$(\d+)'\s*>(.*?)</span>", re.S)


def p_peoplesoft_cg(src):
    url = src["url"]
    sm = re.search(r"[?&]SiteId=(\d+)", url)
    site = sm.group(1) if sm else "1"
    base = url.split("?")[0]
    skip = re.compile(src["skip_family_re"], re.I) if src.get("skip_family_re") else None
    st, txt = _Session().req(url, headers={"Accept-Language": "en-US,en;q=0.9"})
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    rows = {}
    for m in _PS_CG_FIELD_RE.finditer(txt or ""):
        rows.setdefault(int(m.group(2)), {})[m.group(1)] = strip_tags(m.group(3))
    out = []
    for i in sorted(rows):
        r = rows[i]
        jid, title = r.get("HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID", ""), r.get("SCH_JOB_TITLE", "")
        if not (jid and title):
            continue
        fam = r.get("JOB_FAMILY_LABEL", "")
        if skip and skip.search(fam):
            continue
        link = "%s?Page=HRS_APP_JBPST_FL&Action=U&FOCUS=Applicant&SiteId=%s&JobOpeningId=%s&PostingSeq=1" % (base, site, jid)
        snippet = " | ".join(x for x in (r.get("HRS_APP_JBSCH_I_HRS_DEPT_DESCR", ""), fam) if x)
        out.append(item(title, link, r.get("LOCATION", ""), r.get("SCH_OPENED") or None, snippet, "", src["name"]))
    if not out and "SCH_JOB_TITLE$" not in (txt or "") and "jobs found" not in (txt or ""):
        return [], "no result grid"
    return out, None


# ----------------------------------------------------------------------------
# PeopleSoft Candidate Gateway, classic UI behind a portal page (HRS_HRAM.HRS_APP_SCHJOB), e.g. McMaster University.
# url: the portal (psp) search URL. The portal GET signs the browser in as the public guest; its iframe
# "ptifrmtgtframe" (psc) holds the grid and returns the sign-in page when fetched on its own, so both GETs share
# one cookie session. Rows: POSTINGLINK$n ("TITLE - <id>", a postback carrying HRS_JOB_OPEN_ID_PB.<id>) and
# JOBATTRIBUTES$n ("Department: .. | Location: .. | Posted Date: yyyy/mm/dd"). First page only (25 rows, newest
# first). Link: job_url_template with {id} (the portal's Page=HRS_APP_JBPST deep link).
# ----------------------------------------------------------------------------

_PSC_LINK_RE = re.compile(r"id='POSTINGLINK\$(\d+)'[^>]*href=\"[^\"]*HRS_JOB_OPEN_ID_PB\.(\d+)[^\"]*\"[^>]*>(.*?)</a>", re.S)
_PSC_ATTR_RE = re.compile(r"id='JOBATTRIBUTES\$(\d+)'[^>]*>(.*?)</span>", re.S)


def p_peoplesoft_classic(src):
    s = _Session()
    st, txt = s.req(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    fm = re.search(r'id="ptifrmtgtframe"[^>]*\ssrc="([^"]+)"', txt)
    if fm:
        st, txt = s.req(html.unescape(fm.group(1)), headers={"Referer": src["url"]})
        if st is None or st >= 400:
            return [], "iframe HTTP %s" % st
    attrs = {i: strip_tags(a) for i, a in _PSC_ATTR_RE.findall(txt)}
    tmpl = src.get("job_url_template") or ""
    out, seen = [], set()
    for idx, jid, title in _PSC_LINK_RE.findall(txt):
        if jid in seen:
            continue
        seen.add(jid)
        t = re.sub(r"\s*-\s*%s\s*$" % jid, "", strip_tags(title))
        f = {k.strip().lower(): v.strip() for k, v in re.findall(r"([A-Za-z ]+):\s*([^|]+)", attrs.get(idx, ""))}
        link = tmpl.replace("{id}", jid) if tmpl else src.get("careers_url", "")
        out.append(item(t, link, f.get("location", ""), f.get("posted date"),
                        "Department: %s" % f.get("department", ""), "", src["name"]))
    return out, (None if out else "no rows (sign-in page or layout change)")


# ----------------------------------------------------------------------------
# Technomedia / Signature career portal (jobs-ca.technomedia.com/<client>/), e.g. York University.
# Every link is "?_4x" + hex(gzip(query string)) carrying session ids and a signature. The job list
# (processItem=portal.jobposting.j300) renders only inside the session that loaded the home page, so: GET home,
# follow the home page's own j300 link, read the result table (50 rows, newest first). Row links change every
# session; week-to-week identity rests on the fuzzy key (company|title|city).
# ----------------------------------------------------------------------------

def _tm_query(href):
    m = re.match(r"\?_4x([0-9A-Fa-f]+)", href or "")
    if not m:
        return ""
    try:
        return gzip.decompress(binascii.unhexlify(m.group(1))).decode("utf-8", "replace")
    except Exception:
        return ""


def p_technomedia(src):
    base = src["url"]
    s = _Session()
    st, txt = s.req(base)
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    link = None
    for h in re.findall(r'href="(\?_4x[0-9A-Fa-f]+)"', txt):
        q = _tm_query(h)
        if "processItem=portal.jobposting.j300" in q and "showSelection" not in q:
            link = h
            break
    if not link:
        return [], "no job-list link on the home page"
    st, txt = s.req(abs_url(base, link), headers={"Referer": base})
    if st is None or st >= 400:
        return [], "list HTTP %s" % st
    out, seen = [], set()
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", txt, re.S):
        anchors = re.findall(r'<a href="(\?_4x[0-9A-Fa-f]+&(?:amp;)?offerid=(\d+))" class="relink"\s*>(.*?)</a>', row, re.S)
        if not anchors:
            continue
        href, oid, _ = anchors[-1]
        title = strip_tags(anchors[-1][2])
        if not title or oid in seen:
            continue
        seen.add(oid)
        cols = [strip_tags(c) for c in re.findall(r'<div class="styleColAdd\d">(.*?)</div>', row, re.S)]
        dm = re.search(r">\s*(\d{2})-(\d{2})-(\d{4})\s*<", row)
        posted = "%s-%s-%s" % (dm.group(3), dm.group(1), dm.group(2)) if dm else None
        out.append(item(title, abs_url(base, html.unescape(href)), "", posted, " | ".join(c for c in cols if c), "", src["name"]))
    return out, (None if out else "no rows")


# ----------------------------------------------------------------------------
# Cornerstone OnDemand career site (<corp>.csod.com/ux/ats/careersite/<id>/home), e.g. Ontario Tech University.
# The page embeds an anonymous token (csod.context.token) and endpoints.cloud; the site POSTs its searches to
# <cloud>rec-job-search/external/jobs with Authorization: Bearer <token>.
# ----------------------------------------------------------------------------

def p_csod(src):
    m = re.match(r"https://([^.]+)\.csod\.com/ux/ats/careersite/(\d+)/", src["url"])
    if not m:
        return [], "bad csod url"
    corp, site = m.group(1), int(m.group(2))
    st, page = http(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    tm = re.search(r'"token":"([^"]+)"', page)
    cm = re.search(r'"cloud":"([^"]+)"', page)
    if not tm or not cm:
        return [], "no token"
    body = {"careerSiteId": site, "careerSitePageId": site, "pageNumber": 1, "pageSize": 100, "cultureId": 1,
            "searchText": src.get("search_text", ""), "cultureName": "en-US", "states": [], "countryCodes": [], "cities": [],
            "placeID": "", "radius": None, "postingsWithinDays": None, "customFieldCheckboxKeys": [],
            "customFieldDropdowns": [], "customFieldRadios": []}
    j, err = get_json(cm.group(1) + "rec-job-search/external/jobs", "POST", body,
                      {"Authorization": "Bearer " + tm.group(1), "Accept": "application/json",
                       "Origin": "https://%s.csod.com" % corp})
    if j is None:
        return [], err
    out = []
    for r in ((j.get("data") or {}).get("requisitions") or []):
        locs = "; ".join(", ".join(str(l.get(k)) for k in ("city", "state", "country") if l.get(k)) for l in (r.get("locations") or []))
        url = "https://%s.csod.com/ux/ats/careersite/%d/home/requisition/%s?c=%s" % (corp, site, r.get("requisitionId"), corp)
        out.append(item(r.get("displayJobTitle"), url, locs, r.get("postingEffectiveDate"),
                        (r.get("externalDescription") or "")[:400], "", src["name"]))
    return out, None


# ----------------------------------------------------------------------------
# Talcura (<client>.talcura.com/Cands/Jobs.aspx), ASP.NET WebForms, e.g. Sunnybrook Research Institute.
# GET (cookie session) redirects to Jobs.aspx?session=<id>, 10 rows a page. Filtering and paging are postbacks
# with every hidden field: search = __EVENTTARGET ctl00$ContentPlaceHolder1$btnSearch plus the category combobox
# text and its ClientState JSON; next page = ctl00$ContentPlaceHolder1$dpJobsPager$ctl02$NextButton.
# src: category_text / category_value (Sunnybrook Research Institute = 31), keyword (optional), max_pages.
# ----------------------------------------------------------------------------

_TC_ROW_RE = re.compile(r"<a href='(/Cands/Job\.aspx\?JobId=\d+)'>(.*?)</a>", re.S)


def _tc_hidden(txt):
    d = {}
    for m in re.finditer(r'<input[^>]+type="hidden"[^>]*>', txt or "", re.I):
        tag = m.group(0)
        nm = re.search(r'name="([^"]+)"', tag)
        if not nm:
            continue
        vm = re.search(r'value="([^"]*)"', tag)
        d[html.unescape(nm.group(1))] = html.unescape(vm.group(1)) if vm else ""
    return d


def _tc_rows(txt, base, src_name):
    out = []
    for m in _TC_ROW_RE.finditer(txt or ""):
        inner = m.group(2)
        tm = re.search(r'<div class="title">(.*?)</div>', inner, re.S)
        if not tm:
            continue
        title = strip_tags(re.sub(r'<i class="featured">.*?</i>', " ", tm.group(1)))
        posted = None
        dm = re.search(r'<div class="date">\s*Posted on\s*(.*?)</div>', inner, re.S)
        if dm:
            try:
                posted = dt.datetime.strptime(re.sub(r"\s+", " ", strip_tags(dm.group(1))).strip(), "%B %d, %Y").date().isoformat()
            except Exception:
                posted = None
        dsm = re.search(r'<div class="desc">(.*)', inner, re.S)
        desc = strip_tags(re.sub(r'<div class="date">.*?</div>', " ", dsm.group(1))) if dsm else ""
        out.append(item(title, abs_url(base, m.group(1)), "", posted, desc, "", src_name))
    return out


def p_talcura(src):
    s = _Session()
    st, txt = s.req(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    am = re.search(r'<form[^>]+action="([^"]+)"', txt or "")
    if not am:
        return [], "no form"
    post_url = abs_url(src["url"], html.unescape(am.group(1)))
    hdr = {"Content-Type": "application/x-www-form-urlencoded", "Referer": post_url}
    fields = _tc_hidden(txt)
    if src.get("category_value") or src.get("keyword"):
        fields.update({"__EVENTTARGET": "ctl00$ContentPlaceHolder1$btnSearch", "__EVENTARGUMENT": "",
                       "ctl00$ContentPlaceHolder1$tbKeyword": src.get("keyword") or ""})
        if src.get("category_value"):
            fields["ctl00$ContentPlaceHolder1$cbCategory"] = src.get("category_text") or ""
            fields["ctl00_ContentPlaceHolder1_cbCategory_ClientState"] = json.dumps(
                {"logEntries": [], "value": str(src["category_value"]), "text": src.get("category_text") or "",
                 "enabled": True, "checkedIndices": [], "checkedItemsTextOverflows": False}, separators=(",", ":"))
        st, txt = s.req(post_url, "POST", urllib.parse.urlencode(fields), hdr)
        if st is None or st >= 400:
            return [], "search HTTP %s" % st
    out, seen = [], set()
    for _ in range(int(src.get("max_pages") or 10)):
        new = [r for r in _tc_rows(txt, post_url, src["name"]) if r["url"] not in seen]
        for r in new:
            seen.add(r["url"])
            out.append(r)
        tot = re.search(r"Jobs\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d+)", strip_tags(txt or ""))
        if not new or not tot or int(tot.group(2)) >= int(tot.group(3)):
            break
        fields = _tc_hidden(txt)
        fields.update({"__EVENTTARGET": "ctl00$ContentPlaceHolder1$dpJobsPager$ctl02$NextButton", "__EVENTARGUMENT": ""})
        time.sleep(float(src.get("pause_seconds") or 0.5))
        st, txt = s.req(post_url, "POST", urllib.parse.urlencode(fields), hdr)
        if st is None or st >= 400:
            break
    return out, (None if out else "no rows")


# ----------------------------------------------------------------------------
# Attrax career sites (careers.wabtec.com). /vacanciessitemap.xml lists every live job as
# /job/<title-slug>-in-<city-slug>-<state>-<country-slug>-jid-<n>; the /jobs search pages filter with query
# strings, which robots.txt disallows, so the sitemap is the allowed full list. Titles come from the slug
# (accents and punctuation are lost). src: url, optional countries (default ["canada"]), states (default on, qc).
# ----------------------------------------------------------------------------

def p_attrax_sitemap(src):
    st, txt = http(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    countries = [c.lower() for c in (src.get("countries") or ["canada"])]
    states = [x.lower() for x in (src.get("states") or ["on", "qc"])]
    rx = re.compile(r"<loc>\s*(https?://[^<\s]+/job/([a-z0-9-]+)-in-([a-z0-9]+(?:-[a-z0-9]+)*?)-([a-z]{2})-(%s)-jid-(\d+))\s*</loc>"
                    % "|".join(re.escape(c) for c in countries), re.I)
    out = []
    for m in rx.finditer(txt):
        url, tslug, cslug, stc, country, jid = m.groups()
        if states and stc.lower() not in states:
            continue
        title = re.sub(r"\b(ii|iii|iv)\b", lambda x: x.group(1).upper(), tslug.replace("-", " ").strip())
        title = title[:1].upper() + title[1:]
        loc = "%s, %s, %s" % (cslug.replace("-", " ").title(), stc.upper(), country.replace("-", " ").title())
        out.append(item(title, url, loc, None, "", "", src["name"]))
    return out, (None if out else "no matching job urls")


# ----------------------------------------------------------------------------
# Oracle Taleo Social Sourcing / SelectMinds portal (<corp>.referrals.selectminds.com), e.g. Tetra Tech Canada.
# The search is server-side state: GET /jobs/search?all (cookie session) links /ajax/jobs/<sid>/add/<facet>; that
# call (X-Requested-With) answers {"Status": "OK", "Result": <new sid>, "UserMessage": <count>}, and the result
# pages /jobs/search/<new sid>[/page<n>] then read without cookies, 10 rows a page.
# src: url, facet (e.g. "country/7570" = Canada), max_pages.
# ----------------------------------------------------------------------------

_SM_ROW_RE = re.compile(r'<a href="([^"]+)" class="job_link[^"]*">(.*?)</a>(.*?)(?=<a href="[^"]+" class="job_link|$)', re.S)


def p_selectminds(src):
    base = "https://" + urllib.parse.urlsplit(src["url"]).netloc
    s = _Session()
    st, page = s.req(src["url"])
    if st is None or st >= 400:
        return [], "HTTP %s" % st
    m = re.search(r"/ajax/jobs/(\d+)/add/" + re.escape(src["facet"]), page)
    if not m:
        return [], "facet %s not on page" % src["facet"]
    st, js = s.req("%s/ajax/jobs/%s/add/%s" % (base, m.group(1), src["facet"]), headers={"X-Requested-With": "XMLHttpRequest"})
    try:
        res = json.loads(js)
    except Exception:
        return [], "facet call HTTP %s" % st
    sid, total = res.get("Result"), int(res.get("UserMessage") or 0)
    if res.get("Status") != "OK" or not sid:
        return [], "facet call: %s" % js[:120]
    out, seen, errs = [], set(), []
    for n in range(1, int(src.get("max_pages") or 20) + 1):
        st, txt = http("%s/jobs/search/%s" % (base, sid) + ("" if n == 1 else "/page%d" % n))
        if st is None or st >= 400:
            errs.append("HTTP %s" % st)
            break
        got = 0
        for row in _SM_ROW_RE.finditer(txt):
            link = row.group(1)
            if link in seen:
                continue
            seen.add(link)
            got += 1
            lm = re.search(r'<span class="location">(.*?)</span>', row.group(3), re.S)
            cm = re.search(r'<span class="category">(.*?)</span>', row.group(3), re.S)
            dm = re.search(r'<p class="jlr_description">(.*?)</p>', row.group(3), re.S)
            loc = re.sub(r"\s+and \d+ additional locations?$", "", strip_tags(lm.group(1)) if lm else "")
            out.append(item(strip_tags(row.group(2)), link, loc, None,
                            "%s %s" % (strip_tags(cm.group(1)) if cm else "", strip_tags(dm.group(1)) if dm else ""),
                            "", src["name"]))
        if not got or len(out) >= total:
            break
    return out, ("; ".join(errs) if errs and not out else None)


PARSERS = {
    "peoplesoft_cg": p_peoplesoft_cg, "peoplesoft_classic": p_peoplesoft_classic, "technomedia": p_technomedia,
    "csod": p_csod, "talcura": p_talcura, "attrax_sitemap": p_attrax_sitemap, "selectminds": p_selectminds,
    "greenhouse": p_greenhouse, "lever": p_lever, "ashby": p_ashby, "smartrecruiters": p_smartrecruiters,
    "workable_v3": p_workable_v3, "recruitee": p_recruitee, "bamboohr": p_bamboohr, "workday": p_workday,
    "oracle_hcm": p_oracle_hcm, "icims_json": p_icims_json, "eightfold_pcsx": p_eightfold_pcsx, "amazon": p_amazon,
    "apple": p_apple, "successfactors_rss": p_successfactors_rss, "successfactors_html": p_successfactors_html,
    "sitemap_slug": p_sitemap_slug, "json_generic": p_json_generic, "html_generic": p_html_generic,
    "radancy_json": p_radancy_json, "adp_workforcenow": p_adp_workforcenow,
    "dayforce": p_dayforce, "phenom": p_phenom, "taleo_rest": p_taleo_rest, "html_jobs": p_html_jobs,
    "astro_jobs": p_astro_jobs, "workable_widget": p_workable_widget, "taleo_tbe": p_taleo_tbe,
    "successfactors_html_multi": p_successfactors_html_multi, "sf_sitemap": p_sf_sitemap, "json_map": p_json_map,
    "eightfold_pcsx_paged": p_eightfold_pcsx_paged, "jibe": p_jibe, "applicantstack": p_applicantstack,
    "hibob": p_hibob, "html_rows": p_html_rows, "commoninja_jobs": p_commoninja_jobs,
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
    if src.get("skip_title_re") and items:
        skip = re.compile(src["skip_title_re"], re.I)
        items = [it for it in items if not skip.search(it.get("title") or "")]
    default_loc = ", ".join(src.get("cities", [])[:2])
    for it in items:
        it["company"] = it.get("company") or src["name"]
        it["employer_source"] = True
        it["sector"] = src.get("sector", "")
        it["clearance_risk"] = src.get("clearance_risk", "")
        it["french_likely"] = bool(src.get("french_likely"))
        if src.get("unknown_location_ok"):
            it["unknown_location_ok"] = True   # single-site boards that list building names (McGill, uOttawa)
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
    budget = int(cfg.get("max_requests", 400))
    for kw in cfg.get("keywords", []):
        # a keyword is a string (all geos) or {"q": "...", "geos": ["Quebec"]} (French terms, for example)
        geos = cfg.get("geo_ids", {})
        pages = cfg.get("pages", [0])
        if isinstance(kw, dict):
            geos = {g: geos[g] for g in kw.get("geos", geos.keys()) if g in geos}
            pages = kw.get("pages", pages)
            kw = kw["q"]
        for geo_name, geo in geos.items():
            for start in pages:
                if stats["requests"] >= budget:
                    break
                u = ("https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=%s&geoId=%s&f_TPR=r604800&start=%d"
                     % (urllib.parse.quote(kw), geo, start))
                st, txt = http(u, headers={"Accept": "text/html"}, retry_429=False)
                stats["requests"] += 1
                if st == 429:
                    stats["errors"].append("429 at %s/%s; backing off 60s" % (kw, geo_name))
                    time.sleep(60)
                    st, txt = http(u, headers={"Accept": "text/html"}, retry_429=False)
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
    """Job Bank's own Atom feed (its syndication channel; the terms of use forbid scripted searches of the HTML
    pages). One feed request per entry of cfg["feeds"], each a query string, for example NOC-coded:
    fn21=21310&fn21=21311&...&fprov=ON&fprov=QC&sort=D&rows=100&fage=7"""
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    if skip or not cfg.get("enabled", True):
        stats["status"] = "skipped"
        return [], stats
    out, got = [], set()
    for qs in cfg.get("feeds", []):
        u = "https://www.jobbank.gc.ca/jobsearch/feed/jobSearchRSSfeed?" + qs
        st, txt = http(u)
        stats["requests"] += 1
        if st is None or st >= 400:
            stats["errors"].append("%s %s" % (st, qs[:60]))
        else:
            for it in _rss_items(txt, u, "jobbank"):
                if it["url"] in got:
                    continue
                got.add(it["url"])
                if not LOC_RE.search(it["location"] or ""):
                    it["location"] = (it["location"] + " " if it["location"] else "") + "Ontario or Quebec"
                out.append(it)
        time.sleep(float(cfg.get("pace_seconds", 5)))
    stats["n"] = len(out)
    if not out and not stats["errors"]:
        stats["status"] = "empty"
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
    per_board = {}
    for cname, cval in cfg.get("collections", {}).items():
        # value: collection id (Communitech, the original board) or {"id", "origin", "terms"}
        if isinstance(cval, dict):
            cid = cval.get("id")
            origin = (cval.get("origin") or "https://www1.communitech.ca").rstrip("/")
            terms = cval.get("terms") or cfg.get("terms", [])
        else:
            cid, origin, terms = cval, "https://www1.communitech.ca", cfg.get("terms", [])
        n_before = len(out)
        for t in terms:
            u = "https://api.getro.com/api/v2/collections/%s/search/jobs" % cid
            j, err = get_json(u, "POST", {"hitsPerPage": 50, "page": 0, "query": t},
                              {"Origin": origin, "Referer": origin + "/jobs", "Accept": "application/json"})
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
                                _loc_from(locs), h.get("created_at") or h.get("published_at"),
                                " ".join(str(x).replace("_", " ") for x in (h.get("seniority"), h.get("work_mode")) if x),
                                org.get("name", "") if isinstance(org, dict) else str(org), "getro:" + cname))
        per_board[cname] = len(out) - n_before
    stats["n"] = len(out)
    stats["per_board"] = per_board
    if not out and not stats["errors"]:
        stats["status"] = "empty"
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
        status = "no_openings" if closed and not links else ("has_openings" if links else "unclear")
        # signature of what a reader would notice: the status and the set of job-link titles (page chrome ignored)
        sig = status + "|" + "|".join(sorted(set(x["title"].lower() for x in links)))
        res.append({"name": p["name"], "url": p["url"], "status": status,
                    "matched_phrases": closed[:3], "job_links": len(links), "sample_titles": [x["title"] for x in links[:5]],
                    "digest": hashlib.sha1(sig.encode("utf-8")).hexdigest()[:12]})
    return res


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

MADGEX_LOCATIONS = {"Ontario": "4098798", "Quebec": "4098800"}


def run_madgex(cfg, skip):
    """EngineeringCareers.ca (Engineering Institute of Canada) RSS, Madgex platform. Item titles are
    "Employer: Job title"; the feed gives the province only. 20 items per page, newest first with sort=Date."""
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    if skip or not cfg.get("enabled", False):
        stats["status"] = "skipped"
        return [], stats
    host = cfg.get("host", "https://www.engineeringcareers.ca").rstrip("/")
    locs = cfg.get("locations") or MADGEX_LOCATIONS
    out, got = [], set()
    for kw in cfg.get("keywords", [""]):
        for prov, lid in locs.items():
            for page in range(1, int(cfg.get("pages", 1)) + 1):
                q = {"LocationId": lid, "countrycode": "CA", "sort": "Date"}
                if kw:
                    q["keywords"] = kw
                if page > 1:
                    q["page"] = str(page)
                u = host + "/jobsrss/?" + urllib.parse.urlencode(q)
                st, txt = http(u, headers={"Accept": "application/rss+xml, text/xml"})
                stats["requests"] += 1
                time.sleep(float(cfg.get("pace_seconds", 1.0)))
                if st is None or st >= 400:
                    stats["errors"].append("%s %s/%s" % (st, kw, prov))
                    break
                n = 0
                for it in _rss_items(txt, u, "engineeringcareers"):
                    n += 1
                    it["url"] = re.sub(r"\?.*$", "", it["url"])
                    if it["url"] in got:
                        continue
                    got.add(it["url"])
                    company, _, title = it["title"].partition(": ")
                    if title:
                        it["title"], it["company"] = title, company
                    if not LOC_RE.search(it.get("location") or ""):
                        it["location"] = prov
                    it["query"] = kw
                    out.append(it)
                if n < 20:
                    break
    stats["n"] = len(out)
    if not out and not stats["errors"]:
        stats["status"] = "empty"
    return out, stats


ESPRESSO_CARD_RE = re.compile(r'<h2 class="job_index-content_list_item-title"[^>]*>\s*<a href="(/emploi/\d+/[^"]+)"[^>]*>(.*?)</a>'
                              r'(.*?)(?=<h2 class="job_index-content_list_item-title"|$)', re.S)


def _fr_relative_date(s):
    """'Publié il y a 24 jours' / 'il y a 3 heures' / 'il y a 2 semaines' / 'il y a 1 mois' -> ISO date."""
    m = re.search(r"il y a\s+(\d+)\s+(minute|heure|jour|semaine|mois)", s or "", re.I)
    if not m:
        return TODAY.isoformat() if re.search(r"aujourd", s or "", re.I) else None
    n, unit = int(m.group(1)), m.group(2).lower()
    days = {"minute": 0, "heure": 0, "jour": n, "semaine": 7 * n, "mois": 30 * n}[unit]
    return (TODAY - dt.timedelta(days=days)).isoformat()


def run_espresso(cfg, skip):
    """Espresso-Jobs (Quebec tech board), server-rendered search pages (robots.txt allows automated reading of these pages)."""
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    if skip or not cfg.get("enabled", False):
        stats["status"] = "skipped"
        return [], stats
    base = "https://www.espresso-jobs.com"
    out, got = [], set()
    for kw in cfg.get("keywords", []):
        for page in range(1, int(cfg.get("pages", 2)) + 1):
            q = {"keyword": kw}
            if page > 1:
                q["page_no"] = str(page)
            u = base + "/emploi?" + urllib.parse.urlencode(q)
            st, txt = http(u, headers={"Accept-Language": "fr-CA,fr;q=0.9,en;q=0.8"})
            stats["requests"] += 1
            time.sleep(float(cfg.get("pace_seconds", 1.0)))
            if st is None or st >= 400:
                if not (st == 404 and page > 1):
                    stats["errors"].append("%s %s p%d" % (st, kw, page))
                break
            n = 0
            for m in ESPRESSO_CARD_RE.finditer(txt):
                n += 1
                link = base + m.group(1)
                if link in got:
                    continue
                got.add(link)
                tail = m.group(3)[:4000]
                cm = re.search(r'class="job_index-content_list_item-company"[^>]*>(.*?)</p>', tail, re.S)
                lm = re.search(r'class="job-location-info"[^>]*data-province="([^"]*)"[^>]*data-city="([^"]*)"[^>]*>(.*?)</div>', tail, re.S)
                pm = re.search(r'class="publish-time-wrapper"[^>]*>(.*?)</span>', tail, re.S)
                ty = re.search(r'class="job_index-content_list_item_infos-type"[^>]*>(.*?)</p>', tail, re.S)
                loc = ""
                if lm:
                    loc = ", ".join(x for x in (lm.group(2), lm.group(1)) if x) or strip_tags(lm.group(3))
                pub = strip_tags(pm.group(1)) if pm else ""
                out.append(item(strip_tags(m.group(2)), link, loc or "Quebec", _fr_relative_date(pub),
                                "%s %s" % (strip_tags(ty.group(1)) if ty else "", pub), strip_tags(cm.group(1)) if cm else "",
                                "espresso_jobs", {"query": kw}))
            if n < 20:
                break
    stats["n"] = len(out)
    if not out and not stats["errors"]:
        stats["status"] = "empty"
    return out, stats


KEYS = {}   # filled in main() from --keys and the environment


def run_adzuna(cfg, skip):
    """Adzuna Canada job-search API (free key; personal use allowed; 250 calls a day). Listings credit Adzuna."""
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    app_id, app_key = KEYS.get("adzuna_app_id"), KEYS.get("adzuna_app_key")
    if skip or not cfg.get("enabled", True):
        stats["status"] = "skipped"
        return [], stats
    if not (app_id and app_key):
        stats["status"] = "no_key"
        return [], stats
    out, got = [], set()
    for kw in cfg.get("keywords", []):
        for where in cfg.get("where", ["Ontario", "Quebec"]):
            for page in range(1, int(cfg.get("pages", 1)) + 1):
                q = {"app_id": app_id, "app_key": app_key, "what": kw, "where": where, "results_per_page": 50,
                     "max_days_old": int(cfg.get("max_days_old", 8)), "sort_by": "date", "content-type": "application/json"}
                u = "https://api.adzuna.com/v1/api/jobs/ca/search/%d?%s" % (page, urllib.parse.urlencode(q))
                j, err = get_json(u)
                stats["requests"] += 1
                time.sleep(float(cfg.get("pace_seconds", 2.5)))
                if j is None:
                    stats["errors"].append("%s %s/%s" % (err, kw, where))
                    break
                res = j.get("results") or []
                for r in res:
                    link = r.get("redirect_url") or ""
                    key = r.get("id") or link
                    if key in got:
                        continue
                    got.add(key)
                    sal = ""
                    if r.get("salary_min") and r.get("salary_is_predicted") in (0, "0", None, False):
                        sal = " salary %s-%s" % (r.get("salary_min"), r.get("salary_max"))
                    out.append(item(r.get("title"), link, (r.get("location") or {}).get("display_name", ""), r.get("created"),
                                    (r.get("description") or "") + sal, (r.get("company") or {}).get("display_name", ""),
                                    "adzuna", {"query": kw}))
                if len(res) < 50:
                    break
    stats["n"] = len(out)
    if not out and not stats["errors"]:
        stats["status"] = "empty"
    return out, stats


AGO_RE = re.compile(r"(\d+)\+?\s*(minute|hour|day|week|month)s?\s+ago", re.I)


def _ago_date(s):
    m = AGO_RE.search(s or "")
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2).lower()
    days = {"minute": 0, "hour": 0, "day": n, "week": 7 * n, "month": 30 * n}[unit]
    return (TODAY - dt.timedelta(days=days)).isoformat()


def run_serpapi(cfg, skip):
    """Google Jobs through SerpApi (free plan: 250 searches a month). One search returns 10 postings that Google
    collected from employer sites and job boards, each with its apply links."""
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": []}
    key = KEYS.get("serpapi_key")
    if skip or not cfg.get("enabled", True):
        stats["status"] = "skipped"
        return [], stats
    if not key:
        stats["status"] = "no_key"
        return [], stats
    out, got = [], set()
    budget = int(cfg.get("max_searches", 24))
    for qtext in cfg.get("queries", []):
        for loc in cfg.get("locations", ["Ontario, Canada", "Quebec, Canada"]):
            if stats["requests"] >= budget:
                break
            q = {"engine": "google_jobs", "q": qtext, "location": loc, "google_domain": "google.ca", "gl": "ca",
                 "hl": "en", "api_key": key}
            j, err = get_json("https://serpapi.com/search.json?" + urllib.parse.urlencode(q))
            stats["requests"] += 1
            if j is None:
                stats["errors"].append("%s %s/%s" % (err, qtext, loc))
                continue
            for r in j.get("jobs_results") or []:
                jid = r.get("job_id") or r.get("title")
                if jid in got:
                    continue
                got.add(jid)
                ext = r.get("detected_extensions") or {}
                opts = [o.get("link") for o in r.get("apply_options") or [] if isinstance(o, dict) and o.get("link")]
                direct = [o for o in opts if not re.search(r"linkedin\.com|indeed\.|glassdoor\.|ziprecruiter|talent\.com", o)]
                link = (direct or opts or [r.get("share_link") or ""])[0]
                posted = _ago_date(ext.get("posted_at") or " ".join(str(x) for x in r.get("extensions") or []))
                out.append(item(r.get("title"), link, r.get("location", ""), posted,
                                "%s %s %s" % (ext.get("schedule_type", ""), ext.get("salary", ""), strip_tags(r.get("description") or "", 300)),
                                r.get("company_name", ""), "google_jobs", {"query": qtext, "via": r.get("via", ""), "alt_links": opts[:4]}))
    stats["n"] = len(out)
    if not out and not stats["errors"]:
        stats["status"] = "empty"
    return out, stats


def run_feeds(feeds, skip):
    """Plain RSS/Atom job feeds listed under aggregators.feeds: [{"name", "url", "default_location"}]."""
    stats = {"requests": 0, "n": 0, "status": "ok", "errors": [], "per_feed": {}}
    if skip or not feeds:
        stats["status"] = "skipped" if skip else "none"
        return [], stats
    out = []
    for f in feeds:
        st, txt = http(f["url"])
        stats["requests"] += 1
        if st is None or st >= 400:
            stats["errors"].append("%s %s" % (st, f["name"]))
            stats["per_feed"][f["name"]] = 0
            continue
        items = _rss_items(txt, f["url"], "feed:" + f["name"])
        for it in items:
            if not it["location"] and f.get("default_location"):
                it["location"] = f["default_location"]
            if not it["company"] and f.get("company"):
                it["company"] = f["company"]
        stats["per_feed"][f["name"]] = len(items)
        out.extend(items)
        time.sleep(float(f.get("pace_seconds", 1.0)))
    stats["n"] = len(out)
    if not out and not stats["errors"]:
        stats["status"] = "empty"
    return out, stats


def _merge_into(prim, it, src):
    """Fold a duplicate row into the primary one; the employer's own posting wins as the primary link."""
    if it.get("employer_source") and not prim.get("employer_source"):
        old_url = prim.get("url")
        for k in ("title", "url", "location", "posted", "company", "employer_source", "sector", "clearance_risk", "french_likely"):
            if it.get(k) not in (None, ""):
                prim[k] = it[k]
        if old_url and old_url != prim.get("url"):
            prim.setdefault("alt_urls", []).append(old_url)
    elif it.get("url") and it.get("url") != prim.get("url") and it["url"] not in prim.get("alt_urls", []):
        prim.setdefault("alt_urls", []).append(it["url"])
    if src not in prim["sources"]:
        prim["sources"].append(src)
    if not prim.get("snippet") and it.get("snippet"):
        prim["snippet"] = it["snippet"]
    if not prim.get("posted") and it.get("posted"):
        prim["posted"] = it["posted"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", default="sources.json")
    ap.add_argument("--seen", default=None)
    ap.add_argument("--seen-out", default=None, help="write next week's seen state here (v3 line format)")
    ap.add_argument("--out", default="candidates.json")
    ap.add_argument("--stats", default="stats.json")
    ap.add_argument("--raw-out", default=None, help="debug: every fetched row with its filter verdict and score")
    ap.add_argument("--skip", default="", help="comma list: employers,linkedin,eluta,jobbank,vector,getro,hn,ros,github,feeds,engineeringcareers,"
                         "espresso,adzuna,google_jobs,watch")
    ap.add_argument("--only", default="", help="comma list of employer names to run (substring match)")
    ap.add_argument("--quick", action="store_true", help="skip large downloads")
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--max-per-source", type=int, default=300)
    ap.add_argument("--max-per-company", type=int, default=40)
    ap.add_argument("--min-score", type=int, default=40,
                    help="rows with a lower preliminary score are neither written nor remembered")
    ap.add_argument("--cap", type=int, default=1500)
    ap.add_argument("--retention-days", type=int, default=120)
    ap.add_argument("--keys", default=None, help="JSON file with optional API keys (adzuna_app_id, adzuna_app_key, serpapi_key)")
    args = ap.parse_args()
    import os
    for k in ("adzuna_app_id", "adzuna_app_key", "serpapi_key"):
        if os.environ.get(k.upper()):
            KEYS[k] = os.environ[k.upper()].strip()
    if args.keys:
        # JSON {"adzuna_app_id": ...} or plain "name value" / "name=value" lines; Markdown escapes are ignored
        try:
            with open(args.keys, encoding="utf-8") as f:
                txt = f.read().replace("\_", "_")
            try:
                pairs = json.loads(txt).items()
            except ValueError:
                pairs = [re.split(r"[\s=:]+", ln.strip(), 1) for ln in txt.splitlines() if ln.strip()]
            for k, v in pairs:
                k = str(k).strip().lower()
                if k in ("adzuna_app_id", "adzuna_app_key", "serpapi_key") and v:
                    KEYS[k] = str(v).strip().strip('"')
        except Exception as e:
            print("WARN: could not read keys file: %s" % e, file=sys.stderr)
    skip = set(x.strip() for x in args.skip.split(",") if x.strip())
    t_start = time.time()

    with open(args.sources, encoding="utf-8") as f:
        cfg = json.load(f)
    for host, lim in (cfg.get("host_limits") or {}).items():
        HOST_LIMITS[host.lower()] = lim
    for e in cfg.get("employers", []):
        for nm in [e["name"]] + list(e.get("aliases") or []):
            n = norm_company(nm)
            if n:
                TARGET_COMPANIES.add(n)
    seen, seen_fmt = ({}, "none")
    if args.seen:
        seen, seen_fmt = load_seen(args.seen)

    stats = {"run_date": TODAY.isoformat(), "employers": {}, "aggregators": {}, "github": {}, "watch": [], "filters": {},
             "seen_loaded": {"entries": len(seen), "format": seen_fmt}}
    raw = []
    agg = cfg.get("aggregators", {})

    # LinkedIn and Job Bank are paced (they mostly sleep), so they run in their own threads from the start.
    slow_ex = ThreadPoolExecutor(max_workers=2)
    slow = {"linkedin": slow_ex.submit(run_linkedin, agg.get("linkedin", {}), "linkedin" in skip),
            "jobbank": slow_ex.submit(run_jobbank, agg.get("jobbank", {}), "jobbank" in skip)}

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

    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {
            "eluta": ex.submit(run_eluta, agg.get("eluta", {}), "eluta" in skip),
            "vector": ex.submit(run_vector, agg.get("vector", {}), "vector" in skip),
            "getro": ex.submit(run_getro, agg.get("getro", {}), "getro" in skip),
            "hn": ex.submit(run_hn, agg.get("hn_algolia", {}), "hn" in skip),
            "ros": ex.submit(run_ros, agg.get("ros_discourse", {}), "ros" in skip),
            "feeds": ex.submit(run_feeds, agg.get("feeds", []), "feeds" in skip),
            "engineeringcareers": ex.submit(run_madgex, agg.get("engineeringcareers", {}), "engineeringcareers" in skip),
            "espresso": ex.submit(run_espresso, agg.get("espresso", {}), "espresso" in skip),
            "adzuna": ex.submit(run_adzuna, agg.get("adzuna", {}), "adzuna" in skip),
            "google_jobs": ex.submit(run_serpapi, agg.get("google_jobs", {}), "google_jobs" in skip),
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
    for k, fut in slow.items():
        try:
            items, st = fut.result()
        except Exception as e:
            items, st = [], {"status": "error: %s" % e, "n": 0}
        if st.get("status") == "ok" and not items:
            st["status"] = "empty"
        stats["aggregators"][k] = st
        raw.extend(items)
    slow_ex.shutdown(wait=True)

    # watch pages: a page is "changed" when it was observed before and its signature is new
    watch_keys = {}
    if "watch" not in skip:
        stats["watch"] = run_watch(cfg.get("watch_pages", []), cfg.get("no_openings_patterns", []))
        for w in stats["watch"]:
            if w.get("status") == "fetch_failed":
                continue
            nk = key8("watchname|" + w["name"])
            ck = key8("watch|%s|%s" % (w["name"], w.get("digest", "")))
            w["change"] = "first_observation" if nk not in seen else ("unchanged" if ck in seen else "changed")
            watch_keys[nk] = TODAY.isoformat()
            watch_keys[ck] = TODAY.isoformat()

    # filter, score, merge duplicates across sources, drop what earlier weeks already showed
    counts = {"raw": len(raw), "prefiltered": 0, "seen": 0, "below_min_score": 0, "dup": 0, "kept": 0}
    reasons, bands, src_yield = {}, {"0_not_a_fit": 0, "1-39": 0, "40-49": 0, "50-69": 0, "70+": 0}, {}
    keep, by_url, by_fz = {}, {}, {}
    dump = [] if args.raw_out else None
    for it in raw:
        src = it.get("source", "?")
        sy = src_yield.setdefault(src, {"raw": 0, "prefiltered": 0, "ge40": 0, "ge50": 0, "only_source_ge50": 0})
        sy["raw"] += 1
        ok, reason, flags = classify(it, it.get("employer_source", False))
        if not ok:
            reasons[reason] = reasons.get(reason, 0) + 1
            if dump is not None:
                dump.append(dict(it, verdict=reason))
            continue
        sy["prefiltered"] += 1
        counts["prefiltered"] += 1
        uk = key8(seen_key(it))
        fz = fuzzy_key(it)
        fk = key8(fz) if fz else None
        sc = prelim_score(it, [src])
        p = sc["prelim"]
        bands["0_not_a_fit" if p == 0 else "1-39" if p < 40 else "40-49" if p < 50 else "50-69" if p < 70 else "70+"] += 1
        if dump is not None:
            dump.append(dict(it, verdict="ok", prelim=p, family=sc["family"], family_basis=sc["family_basis"],
                             role_type=sc["role_type"], fuzzy=fz))
        if uk in seen or (fk and fk in seen):
            counts["seen"] += 1
            continue
        if p < args.min_score:
            counts["below_min_score"] += 1
            continue
        sy["ge40"] += 1
        if p >= 50:
            sy["ge50"] += 1
        ident = by_url.get(uk) or (by_fz.get(fk) if fk else None)
        if ident is not None:
            counts["dup"] += 1
            prim = keep[ident]
            _merge_into(prim, it, src)
            if p > prim["prelim"]:
                prim.update(prelim=p, family=sc["family"], role_type=sc["role_type"], score_parts=sc["parts"])
            prim["flags"] = sorted(set(prim["flags"]) | set(flags))
            by_url[uk] = ident
            if fk:
                by_fz[fk] = ident
            continue
        it.update(key=uk, fkey=fk, flags=flags, sources=[src], alt_urls=[], prelim=p, family=sc["family"],
                  role_type=sc["role_type"], score_parts=sc["parts"])
        keep[uk] = it
        by_url[uk] = uk
        if fk:
            by_fz[fk] = uk
    for x in keep.values():
        if x["prelim"] >= 50 and len(x["sources"]) == 1:
            src_yield[x["sources"][0]]["only_source_ge50"] += 1
    counts["kept"] = len(keep)
    stats["filters"] = {"counts": counts, "dropped_by_reason": reasons, "score_bands_new_and_seen": bands}

    cands = sorted(keep.values(), key=lambda x: (x["prelim"], "entry_wording" in x.get("flags", []), x.get("posted") or ""),
                   reverse=True)
    per_company, balanced = {}, []
    for x in cands:
        c = norm_company(x.get("company") or "") or (x.get("company") or "").lower()
        per_company[c] = per_company.get(c, 0) + 1
        if per_company[c] <= args.max_per_company:
            balanced.append(x)
    stats["filters"]["company_capped"] = len(cands) - len(balanced)
    cands = balanced
    if len(cands) > args.cap:
        stats["filters"]["capped_from"] = len(cands)
        cands = cands[: args.cap]
    stats["source_yield"] = src_yield
    stats["keys_present"] = sorted(KEYS)
    stats["candidates_ge50"] = sum(1 for x in cands if x["prelim"] >= 50)

    # next week's state: entries younger than the retention window, every written row (one key each:
    # the cross-source key when there is one, else the URL key) and the watch-page signatures
    if args.seen_out:
        cutoff = (TODAY - dt.timedelta(days=args.retention_days)).isoformat()
        nxt = {k: d for k, d in seen.items() if isinstance(d, str) and d >= cutoff}
        for x in cands:
            nxt[x["fkey"] or x["key"]] = TODAY.isoformat()
        nxt.update(watch_keys)
        write_seen(args.seen_out, nxt)
        stats["seen_out"] = {"path": args.seen_out, "entries": len(nxt), "carried": sum(1 for k in nxt if k in seen),
                             "format": "v3"}
    stats["elapsed_secs"] = round(time.time() - t_start, 1)
    stats["candidates_written"] = len(cands)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(cands, f, ensure_ascii=False, indent=1)
    with open(args.stats, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=1)
    if dump is not None:
        with open(args.raw_out, "w", encoding="utf-8") as f:
            json.dump(dump, f, ensure_ascii=False)
    ok_emp = sum(1 for v in stats["employers"].values() if v.get("status") == "ok")
    print("done: %d raw -> %d candidates (%d scoring 50+); employers ok %d/%d; elapsed %ss"
          % (len(raw), len(cands), stats["candidates_ge50"], ok_emp, len(stats["employers"]), stats["elapsed_secs"]))


if __name__ == "__main__":
    main()
