"""
Churn Radar — point it at a B2B company's domain and it maps their publicly
documented customers, then sweeps the live web for account-specific churn
signals using Exa's search stack.

Exa endpoints used:
  /answer      — vendor profile + customer discovery (structured via outputSchema)
  /findSimilar — competitor discovery from the vendor homepage
  /search      — five churn-signal lanes per customer (semantic queries,
                 includeDomains scoping, category & date filters, highlights
                 + schema summaries in one call)

Run:  EXA_API_KEY=... python app.py   (or put the key in .env)
Open: http://localhost:8000
"""

import asyncio
import json
import os
import re
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

ROOT = Path(__file__).parent
CACHE_DIR = ROOT / "cache"
CACHE_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------- config


def _load_env():
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


_load_env()
EXA_API_KEY = os.environ.get("EXA_API_KEY", "")
EXA_BASE = "https://api.exa.ai"

MAX_CONCURRENCY = 8          # stay well under Exa's 10 QPS
MIN_CALL_SPACING = 0.13      # seconds between call starts
LANE_RESULTS = 4             # results fetched per signal lane

# public-deployment guard: a live scan spends real API money (~$0.60), so cap
# how fast strangers can trigger them. Cached replays cost nothing and are
# never throttled. In-memory state — run this app as a single worker.
LIVE_SCANS_PER_HOUR = int(os.environ.get("LIVE_SCANS_PER_HOUR", "12"))
LIVE_SCANS_PER_IP_PER_HOUR = int(os.environ.get("LIVE_SCANS_PER_IP_PER_HOUR", "4"))
MAX_CONCURRENT_SCANS = 2
_scan_log: deque = deque()          # start times of recent live scans
_scan_log_by_ip: dict[str, deque] = {}
_active_scans = 0


def live_scan_gate(ip: str, has_own_key: bool):
    """Returns an error string if this live scan should be refused, else
    records the scan and returns None. Visitors bringing their own Exa key
    spend their own credits, so only the concurrency cap applies to them."""
    if _active_scans >= MAX_CONCURRENT_SCANS:
        return ("Two live scans are already running — give them a minute to finish, "
                "or use Replay cached for an instant (free) result.")
    if has_own_key:
        return None
    now = time.time()
    while _scan_log and now - _scan_log[0] > 3600:
        _scan_log.popleft()
    per_ip = _scan_log_by_ip.setdefault(ip, deque())
    while per_ip and now - per_ip[0] > 3600:
        per_ip.popleft()
    if len(_scan_log) >= LIVE_SCANS_PER_HOUR or len(per_ip) >= LIVE_SCANS_PER_IP_PER_HOUR:
        return ("Hourly live-scan budget reached (each live sweep spends the demo's API "
                "credits). Try Replay cached, add your own Exa key in the API key panel, "
                "or come back in a bit.")
    _scan_log.append(now)
    per_ip.append(now)
    return None


app = FastAPI(title="Churn Radar")

# ---------------------------------------------------------------- exa client


class ExaClient:
    """Thin async Exa client: rate-spaced, retrying, cost-accounting."""

    def __init__(self, emit, api_key: str | None = None):
        self.emit = emit                      # push events to the SSE stream
        self.sem = asyncio.Semaphore(MAX_CONCURRENCY)
        self.gate = asyncio.Lock()
        self.last_start = 0.0
        self.calls = 0
        self.cost = 0.0
        self.auth_error = False               # set on 401/403 from Exa
        self.http = httpx.AsyncClient(
            base_url=EXA_BASE,
            headers={"x-api-key": api_key or EXA_API_KEY, "Content-Type": "application/json"},
            timeout=45.0,
        )

    async def close(self):
        await self.http.aclose()

    async def post(self, endpoint: str, payload: dict, tag: str):
        async with self.sem:
            for attempt in range(4):
                # space out call starts so bursts never trip the 10 QPS limit
                async with self.gate:
                    wait = self.last_start + MIN_CALL_SPACING - time.monotonic()
                    if wait > 0:
                        await asyncio.sleep(wait)
                    self.last_start = time.monotonic()
                t0 = time.monotonic()
                try:
                    r = await self.http.post(endpoint, json=payload)
                except httpx.HTTPError as e:
                    if attempt == 3:
                        await self.emit({"type": "call", "endpoint": endpoint,
                                         "tag": tag, "ms": 0, "cost": 0,
                                         "error": str(e)})
                        return None
                    await asyncio.sleep(0.6 * (attempt + 1))
                    continue
                ms = int((time.monotonic() - t0) * 1000)
                if r.status_code in (401, 403):
                    self.auth_error = True    # bad key — retrying won't help
                    await self.emit({"type": "call", "endpoint": endpoint,
                                     "tag": tag, "ms": ms, "cost": 0,
                                     "error": f"HTTP {r.status_code} — API key rejected"})
                    return None
                if r.status_code == 429 or r.status_code >= 500:
                    if attempt == 3:
                        await self.emit({"type": "call", "endpoint": endpoint,
                                         "tag": tag, "ms": ms, "cost": 0,
                                         "error": f"HTTP {r.status_code}"})
                        return None
                    await asyncio.sleep(0.8 * (attempt + 1))
                    continue
                if r.status_code != 200:
                    await self.emit({"type": "call", "endpoint": endpoint,
                                     "tag": tag, "ms": ms, "cost": 0,
                                     "error": f"HTTP {r.status_code}: {r.text[:120]}"})
                    return None
                data = r.json()
                cost = ((data.get("costDollars") or {}).get("total")) or 0.0
                self.calls += 1
                self.cost += cost
                await self.emit({"type": "call", "endpoint": endpoint, "tag": tag,
                                 "ms": ms, "cost": round(cost, 4)})
                return data
        return None


# ---------------------------------------------------------------- helpers

DOMAIN_RE = re.compile(r"^(?:https?://)?(?:www\.)?([a-z0-9][a-z0-9.-]*\.[a-z]{2,})(?:[/?#].*)?$", re.I)


def normalize_domain(raw: str):
    m = DOMAIN_RE.match(raw.strip().lower())
    return m.group(1) if m else None


def parse_json_maybe(v):
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (json.JSONDecodeError, ValueError):
            return None
    return None


def name_tokens(name: str):
    """(token, case_sensitive) pairs that count as 'the customer is actually
    named on this page'. Brand names that are capitalized common nouns
    ('Cognition') must match case-sensitively so the noun doesn't count."""
    base = name.strip()
    toks = {base, base.replace(" ", "")}
    first = base.split()[0] if base.split() else base
    if len(first) >= 4 or not first.isalpha():   # keep '11x', drop bare 'The'
        toks.add(first)
    out = []
    for t in toks:
        if len(t) < 3:
            continue
        cs = t.isalpha() and t[0].isupper()
        out.append((t if cs else t.lower(), cs))
    return out


def mentions_name(text: str, tokens):
    """Word-boundary match — 'monday.com' must not match the weekday, 'stack'
    must not match every engineering post, 'Cognition' not the noun."""
    raw = text or ""
    low = raw.lower()
    for tok, cs in tokens:
        hay = raw if cs else low
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(tok)}(?![A-Za-z0-9])", hay):
            return True
    return False


def recency_factor(published: str | None):
    if not published:
        return 0.8  # undated pages (careers, docs) still count, discounted
    try:
        dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
    except ValueError:
        return 0.8
    age = (datetime.now(timezone.utc) - dt).days
    if age <= 183:
        return 1.0
    if age <= 365:
        return 0.85
    if age <= 548:
        return 0.6
    return 0.35


def iso_months_ago(months: int):
    return (datetime.now(timezone.utc) - timedelta(days=30 * months)).strftime(
        "%Y-%m-%dT00:00:00.000Z")


def result_domain(url: str):
    m = re.match(r"https?://(?:www\.)?([^/]+)", url or "")
    return m.group(1).lower() if m else ""


# ---------------------------------------------------------------- pipeline

PROFILE_SCHEMA = {
    "type": "object",
    "properties": {
        "company_name": {"type": "string"},
        "one_liner": {"type": "string", "description": "what the company sells, one sentence"},
        "product_category": {"type": "string", "description": "short category label, e.g. 'web search API for AI applications'"},
        "capability_phrase": {"type": "string", "description": "the capability a customer would have to build themselves to replace this vendor, e.g. 'web-scale crawling, search index and semantic retrieval infrastructure'"},
        "buyer_persona": {"type": "string", "description": "who inside a customer org buys/uses this"},
        "competitors": {"type": "array", "items": {"type": "object", "properties": {
            "name": {"type": "string"}, "domain": {"type": "string"}},
            "required": ["name", "domain"]},
            "description": "5-8 named direct competitors with domains"},
        "churn_modes": {"type": "array", "items": {"type": "string"},
                        "description": "3-5 concrete ways a customer stops needing this vendor"},
    },
    "required": ["company_name", "one_liner", "product_category",
                 "capability_phrase", "competitors", "churn_modes"],
}

CUSTOMERS_SCHEMA = {
    "type": "object",
    "properties": {"customers": {"type": "array", "items": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "domain": {"type": "string"},
            "evidence": {"type": "string", "description": "one line: how we know they are a customer"},
        },
        "required": ["name", "domain", "evidence"],
    }}},
    "required": ["customers"],
}

CASE_STUDY_SUMMARY = {
    "query": "Which single customer company is this page about, and what do they use the vendor's product for? If this page is not a customer case study/story, set customer_name to empty string.",
    "schema": {
        "type": "object",
        "properties": {
            "customer_name": {"type": "string"},
            "customer_domain": {"type": "string"},
            "use_case": {"type": "string"},
        },
        "required": ["customer_name", "customer_domain", "use_case"],
    },
}


def web_mentions_summary(vendor_name):
    return {
        "query": (f"List every company this page documents as a CUSTOMER or production "
                  f"user of {vendor_name} (not partners of other companies, not "
                  f"{vendor_name} itself). Only companies the page gives concrete "
                  f"evidence for. Empty list if none."),
        "schema": {
            "type": "object",
            "properties": {"customers": {"type": "array", "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "domain": {"type": "string"},
                    "evidence": {"type": "string"},
                },
                "required": ["name", "domain", "evidence"],
            }}},
            "required": ["customers"],
        },
    }


# The default signal lanes. The UI lets the user relabel, reweight, redescribe,
# remove, and add lanes; a builtin lane keeps its hand-tuned Exa query as long
# as its description is untouched — edit the description and the lane switches
# to a generic query built from your words.
DEFAULT_SIGNALS = [
    {"id": "competitor_adoption", "label": "COMPETITOR ADOPTION", "short": "COMPETITOR",
     "color": "#FF6A6A", "weight": 3.0,
     "desc": "Your account named on a competitor's changelog, integration docs or case study."},
    {"id": "in_house_build", "label": "BUILDING IN-HOUSE", "short": "IN-HOUSE",
     "color": "#B388FF", "weight": 3.0,
     "desc": "An engineering blog or talk about building the capability you sell them."},
    {"id": "hiring_to_replace", "label": "HIRING TO REPLACE", "short": "HIRING",
     "color": "#8FA5FF", "weight": 2.0,
     "desc": "A job posting whose responsibilities overlap your product."},
    {"id": "shopping_around", "label": "EVALUATING ALTERNATIVES", "short": "ALTERNATIVES",
     "color": "#E9B44C", "weight": 2.0,
     "desc": "Benchmarks or comparisons of providers in your category."},
    {"id": "budget_distress", "label": "BUDGET / STRATEGY", "short": "BUDGET",
     "color": "#9AA7BD", "weight": 1.5,
     "desc": "Layoffs, cost cuts, an acquisition or a pivot, inside an eight-month news window."},
]
DEFAULT_DESCS = {s["id"]: s["desc"] for s in DEFAULT_SIGNALS}
CANON_SHORTS = {s["label"]: s["short"] for s in DEFAULT_SIGNALS}
HEX_COLOR_RE = re.compile(r"^#[0-9a-fA-F]{6}$")


def parse_signals(raw):
    """Sanitize the signal-lane config sent by the UI. Returns (signals,
    is_default). None or garbage → the default set."""
    if not isinstance(raw, list) or not raw:
        return [dict(s) for s in DEFAULT_SIGNALS], True
    out = []
    for i, s in enumerate(raw[:8]):
        if not isinstance(s, dict):
            continue
        label = re.sub(r"\s+", " ", str(s.get("label", ""))).strip()[:40] or f"SIGNAL {i + 1}"
        desc = re.sub(r"\s+", " ", str(s.get("desc", ""))).strip()[:240]
        try:
            weight = min(5.0, max(0.5, float(s.get("weight", 1))))
        except (TypeError, ValueError):
            weight = 1.0
        color = str(s.get("color", "")) if HEX_COLOR_RE.match(str(s.get("color", ""))) else "#8FA5FF"
        sid = s.get("id") if s.get("id") in DEFAULT_DESCS else None
        derived = re.sub(r"[^A-Za-z0-9/+ -]", "", label).upper().split()
        short = CANON_SHORTS.get(label) or (derived[0] if derived else f"LANE{i + 1}")[:12]
        out.append({"id": sid, "label": label, "desc": desc, "weight": weight,
                    "color": color, "short": short})
    if not out:
        return [dict(s) for s in DEFAULT_SIGNALS], True
    is_default = len(out) == len(DEFAULT_SIGNALS) and all(
        a["id"] == b["id"] and a["desc"] == b["desc"]
        and a["weight"] == b["weight"] and a["label"] == b["label"]
        for a, b in zip(out, DEFAULT_SIGNALS))
    return out, is_default


def lane_definitions(profile: dict, competitor_domains: list[str], signals: list[dict]):
    """Build the signal lanes from the user's config. Builtin lanes keep their
    hand-tuned queries phrased from the vendor profile; edited or added lanes
    get a generic semantic query built from the lane's description. Returns
    (lanes_for, lanes_meta)."""
    cname = profile["company_name"]
    cat = profile["product_category"]
    cap = profile["capability_phrase"]
    comp_names = ", ".join(c["name"] for c in profile["competitors"][:6])

    def summary_for(question):
        return {
            "query": (
                f"TASK: Scan this ENTIRE page top to bottom — including changelog "
                f"entries, bullet lists, tables and footers. {question} "
                f"Set relevance to: 'strong' if the page is direct evidence of that, "
                f"'weak' if it is only indirect or partial evidence, 'none' if the page "
                f"is not about this at all or is a mere news mention. In 'evidence' quote "
                f"or tightly paraphrase the single most damning sentence, with names. "
                f"Set mentions_vendor to true ONLY if the literal word '{cname}' "
                f"appears in the page text itself."
            ),
            "schema": {
                "type": "object",
                "properties": {
                    "relevance": {"type": "string", "enum": ["strong", "weak", "none"]},
                    "evidence": {"type": "string"},
                    "mentions_vendor": {"type": "boolean"},
                },
                "required": ["relevance", "evidence", "mentions_vendor"],
            },
        }

    # resolve final lane ids once: a builtin id survives only while its
    # description is untouched — edited descriptions get a custom id so the
    # builtin-specific judging rules (funding guard, changelog override)
    # don't misfire on criteria they weren't written for
    for i, sig in enumerate(signals):
        if sig.get("id") and sig["desc"] == DEFAULT_DESCS[sig["id"]]:
            sig["_lane"] = sig["id"]
        else:
            slug = re.sub(r"[^a-z0-9]+", "_", sig["label"].lower()).strip("_")[:24]
            sig["_lane"] = f"custom_{i}_{slug or 'lane'}"
    lanes_meta = [{"id": s["_lane"], "label": s["label"], "short": s["short"],
                   "color": s["color"], "weight": s["weight"]} for s in signals]

    def lanes_for(customer):
        n = customer["name"]
        builtin = [
            {
                "id": "competitor_adoption",
                "label": "Competitor adoption",
                "weight": 3.0,
                # Exa treats includeDomains as a strong preference, not a hard
                # constraint — off-list results get backfilled when in-domain
                # matches run thin, so we re-enforce the scope client-side
                "strict_domains": competitor_domains,
                "payload": {
                    "query": f"{n} featured as a customer, integration, or partner",
                    "includeDomains": competitor_domains,
                    "numResults": LANE_RESULTS,
                    "contents": {
                        "highlights": {"numSentences": 2, "query": f"{n} integration partner customer"},
                        "summary": summary_for(
                            f"Does this page show the company '{n}' actually USING, integrating "
                            f"with, or partnering with THIS website's own product (a competitor "
                            f"of {cname} in {cat})? Changelog entries, integration docs, case "
                            f"studies and partnership announcements count as strong. A news or "
                            f"editorial article that merely mentions {n} counts as none. A "
                            f"tutorial showing how to plug {n}'s OWN product into apps built on "
                            f"this site (i.e. {n} used as a tool, not as a customer) = none."),
                    },
                },
            },
            {
                "id": "in_house_build",
                "label": "Building in-house",
                "weight": 3.0,
                "payload": {
                    "query": f"{n} engineering blog building their own in-house {cap} instead of using a third-party {cat}",
                    "numResults": LANE_RESULTS,
                    "startPublishedDate": iso_months_ago(15),
                    "contents": {
                        "highlights": {"numSentences": 2, "query": f"{n} building in-house {cap}"},
                        "summary": summary_for(
                            f"Is this page evidence that '{n}' — that specific company, not anyone "
                            f"else — is building or has built IN-HOUSE capability overlapping what "
                            f"{cname} sells ({cap}), reducing their need for an external {cat}? "
                            f"{n}'s own engineering blog posts, talks, or reputable coverage of "
                            f"{n}'s internal systems count as strong ONLY when the system overlaps "
                            f"that specific capability. Internal databases, generic data platforms, "
                            f"or infrastructure unrelated to {cat} = none. Pages about OTHER "
                            f"companies' systems = none."),
                    },
                },
            },
            {
                "id": "hiring_to_replace",
                "label": "Hiring to replace",
                "weight": 2.0,
                "payload": {
                    "query": f"{n} job posting hiring engineers to build {cap}",
                    "numResults": LANE_RESULTS,
                    "contents": {
                        "highlights": {"numSentences": 2, "query": f"{n} hiring {cap} engineer"},
                        "summary": summary_for(
                            f"Is this a SPECIFIC job posting at '{n}' whose stated responsibilities "
                            f"EXPLICITLY include building capability that overlaps what {cname} "
                            f"sells ({cap})? strong ONLY if the posting text names such "
                            f"responsibilities. weak if the role is adjacent (ML platform, data "
                            f"infrastructure) without explicit overlap. none for: general careers "
                            f"pages listing many roles, and generic software/product/IT/sales roles."),
                    },
                },
            },
            {
                "id": "shopping_around",
                "label": "Evaluating alternatives",
                "weight": 2.0,
                "payload": {
                    "query": f"{n} comparing, benchmarking, or switching between {cat} providers such as {comp_names}",
                    "numResults": LANE_RESULTS,
                    "startPublishedDate": iso_months_ago(12),
                    "contents": {
                        "highlights": {"numSentences": 2, "query": f"{n} comparing alternatives {cat}"},
                        "summary": summary_for(
                            f"Is this page evidence that '{n}' specifically is benchmarking, "
                            f"comparing, or publicly evaluating multiple {cat} providers "
                            f"(e.g. {comp_names}), or complaining about their current provider? "
                            f"The page must name '{n}' as the one doing the evaluating — generic "
                            f"'best tools' listicles and benchmarks that don't involve {n} = none."),
                    },
                },
            },
            {
                "id": "budget_distress",
                "label": "Budget / strategy risk",
                "weight": 1.5,
                "payload": {
                    "query": f"{n} layoffs, cost cutting, restructuring, acquisition, or pivot away from AI products",
                    "category": "news",
                    "numResults": LANE_RESULTS,
                    "startPublishedDate": iso_months_ago(8),
                    "contents": {
                        "highlights": {"numSentences": 2, "query": f"{n} layoffs cost cutting acquisition pivot"},
                        "summary": summary_for(
                            f"Is this news evidence that '{n}' faces budget pressure or strategy "
                            f"change that would cut spend on tools like {cname}? BEING ACQUIRED "
                            f"(procurement gets consolidated by the acquirer) = strong. Major "
                            f"layoffs or cost-cutting programs = strong. Pivot away from the "
                            f"products that need a {cat} = strong. '{n}' raising funding or "
                            f"'{n}' itself acquiring another company = none (that is growth, not "
                            f"distress). Routine earnings commentary = weak. News about a "
                            f"different company = none."),
                    },
                },
            },
        ]
        by_id = {b["id"]: b for b in builtin}
        lanes = []
        for sig in signals:
            if sig["_lane"] in by_id:
                lane = dict(by_id[sig["_lane"]])
            else:
                d = sig["desc"] or sig["label"].title()
                lane = {
                    "id": sig["_lane"],
                    "payload": {
                        "query": f"{n}: {d}",
                        "numResults": LANE_RESULTS,
                        "contents": {
                            "highlights": {"numSentences": 2, "query": f"{n} {d[:80]}"},
                            "summary": summary_for(
                                f"Is this page direct evidence of the following churn-risk "
                                f"signal for '{n}' (a customer of {cname}): \"{d}\"? strong = "
                                f"direct evidence of exactly that, about {n} specifically; "
                                f"weak = indirect or partial; none = unrelated, about a "
                                f"different company, or a mere passing mention."),
                        },
                    },
                }
            lane["label"] = sig["label"]
            lane["color"] = sig["color"]
            lane["weight"] = sig["weight"]
            lane["max_evidence"] = 1 if lane["id"] == "hiring_to_replace" else 2
            lanes.append(lane)
        return lanes

    return lanes_for, lanes_meta


PRODUCT_PATH_RE = re.compile(r"/(changelog|integrations?|partners?|customers?|case-stud|docs|blog)", re.I)
FUNDING_RE = re.compile(r"\brais(?:e[sd]?|ing)\b.{0,60}(?:\$|million|billion|funding|round)"
                        r"|series [a-f]\b|\bvaluation\b", re.I)
DISTRESS_RE = re.compile(r"layoffs?|lay(?:ing)? off|acquir|acquisition|merger|cost[ -]cut"
                         r"|restructur|shut(?:ting)? down|winds? down|pivot", re.I)
INTEGRATION_HINT_RE = re.compile(r"integrat|partner|connect|marketplace|\bmcp\b|plugin"
                                 r"|case stud|customer|now available|supports?\b", re.I)
ACQUIRE_VERB = r"(?:acquires?|acquired|buys?|bought|to acquire|snaps up)"
GENERIC_NEGATION_RE = re.compile(r"would count as none|is not about|general careers page"
                                 r"|does not list a specific job|not a (?:job posting|case study)", re.I)


def self_negating(evidence_text: str, tokens):
    """The summarizer sometimes returns relevance=weak while its own evidence
    sentence says the page isn't about the customer. Trust the sentence.
    (Negations about the VENDOR are expected — real churn evidence rarely
    names the vendor — so only customer-negations count.)"""
    t = (evidence_text or "").lower()
    if GENERIC_NEGATION_RE.search(t):
        return True
    for tok, _cs in tokens:
        if re.search(rf"(?:does not|doesn't|do not|no)\s+(?:\w+\s+){{0,3}}"
                     rf"(?:mention|reference|discuss|evidence|indication)\w*\s*"
                     rf"(?:of|to|about)?\s*{re.escape(tok.lower())}", t):
            return True
    return False


def judge_result(r: dict, lane: dict, tokens, customer: dict):
    customer_domain = customer["domain"]
    """Arbitrate between the extractive highlight (ground truth for relevance)
    and the abstractive summary (classification). Returns evidence dict or None."""
    s = parse_json_maybe(r.get("summary")) or {}
    relevance = s.get("relevance", "none")
    highlight = " ".join(r.get("highlights") or [])
    dom = result_domain(r.get("url", ""))
    # hard relevance gate: the page must visibly concern THIS customer —
    # named in the title/highlight text, or hosted on the customer's own domain
    named_in_page = (mentions_name(highlight, tokens)
                     or mentions_name(r.get("title", ""), tokens)
                     or dom.endswith(customer_domain))
    if not named_in_page:
        return None
    # a funding round is growth, not distress — drop unless the page also
    # carries real distress language (layoffs, acquisition, cost cuts)
    if lane["id"] == "budget_distress":
        probe = f"{r.get('title', '')} {s.get('evidence', '')} {highlight}"
        if FUNDING_RE.search(probe) and not DISTRESS_RE.search(probe):
            return None
        # 'Cognition acquires TierZero' is the customer GROWING, not distress —
        # only being acquired counts
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(customer['name'])}(?![A-Za-z0-9])"
                     rf"\W{{0,20}}{ACQUIRE_VERB}", probe, re.I):
            return None
    if relevance == "none":
        if lane["id"] == "competitor_adoption":
            # only trust the extractive-highlight override when the customer is
            # named on the competitor's PRODUCT surface (changelog, integrations,
            # docs), the highlight also carries integration language, and the
            # page is recent — this exists for one case: long changelogs that
            # defeat the summarizer (e.g. a 'Devin — connect via MCP' entry)
            if not (PRODUCT_PATH_RE.search(r.get("url", ""))
                    and INTEGRATION_HINT_RE.search(highlight)
                    and recency_factor(r.get("publishedDate")) >= 0.85):
                return None
            relevance = "strong"
        else:
            # named on the page but the summarizer saw nothing — long pages
            # defeat summarizers, keep it as weak with the raw highlight
            relevance = "weak"

    if relevance == "weak" and self_negating(s.get("evidence", ""), tokens):
        return None
    strength = 1.0 if relevance == "strong" else 0.35
    rec = recency_factor(r.get("publishedDate"))
    # counter-signal: page names the vendor itself → may be powered BY the
    # vendor rather than replacing it; halve the points and flag for review
    mentions_vendor = bool(s.get("mentions_vendor"))
    factor = 0.5 if mentions_vendor else 1.0
    return {
        "lane": lane["id"],
        "lane_label": lane["label"],
        "url": r.get("url"),
        "title": r.get("title") or result_domain(r.get("url", "")),
        "source": result_domain(r.get("url", "")),
        "date": (r.get("publishedDate") or "")[:10] or None,
        "relevance": relevance,
        "evidence": (s.get("evidence") or highlight or "")[:400],
        "mentions_vendor": mentions_vendor,
        "points": round(lane["weight"] * strength * rec * factor, 2),
    }


def tier_for(score: float, strongest: float):
    # red requires real weight AND at least one strong, recent signal —
    # a pile of weak signals can only ever reach 'watch'
    if score >= 5.0 and strongest >= 1.5:
        return "at_risk"
    if score >= 1.8:
        return "watch"
    return "healthy"


async def sweep_customer(exa: ExaClient, customer, lanes_for, vendor_domain, emit):
    tokens = name_tokens(customer["name"])
    lanes = lanes_for(customer)
    seen_urls: set = set()

    async def run_lane(lane):
        data = await exa.post("/search", lane["payload"], f"{lane['id']}:{customer['name']}")
        kept = []
        if data:
            strict = lane.get("strict_domains")
            for r in data.get("results", []):
                url = r.get("url", "")
                dom = result_domain(url)
                if not url or url in seen_urls or dom.endswith(vendor_domain):
                    continue  # vendor's own pages are never churn evidence
                if strict and not any(dom == d or dom.endswith("." + d) for d in strict):
                    continue  # enforce includeDomains scope ourselves
                item = judge_result(r, lane, tokens, customer)
                if item:
                    seen_urls.add(url)
                    kept.append(item)
        # keep only the strongest evidence per lane so one noisy lane can't
        # dominate the verdict
        kept.sort(key=lambda x: -x["points"])
        return lane, kept[:lane.get("max_evidence", 2)]

    # lanes stream independently — the board lights up cell by cell as each
    # lane's evidence lands, instead of waiting for the whole account
    evidence, done = [], 0
    for fut in asyncio.as_completed([run_lane(lane) for lane in lanes]):
        lane, kept = await fut
        done += 1
        evidence.extend(kept)
        await emit({"type": "signal", "domain": customer["domain"], "lane": lane["id"],
                    "evidence": kept, "lanes_done": done, "lanes_total": len(lanes)})
    score = round(sum(e["points"] for e in evidence), 2)
    strongest = max((e["points"] for e in evidence), default=0.0)
    verdict = {
        "customer": customer,
        "score": score,
        "tier": tier_for(score, strongest),
        "evidence": sorted(evidence, key=lambda x: -x["points"]),
        # the verbatim per-lane requests that swept this account — surfaced
        # by the account API pill in the UI
        "api": [{"lane": l["id"], "label": l["label"], "color": l.get("color"),
                 "endpoint": "/search", "payload": l["payload"]} for l in lanes],
    }
    await emit({"type": "verdict", "data": verdict})
    return verdict


RESOLVE_SCHEMA = {
    "type": "object",
    "properties": {"customers": {"type": "array", "items": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "commonly used brand name with canonical casing, e.g. HubSpot"},
            "domain": {"type": "string", "description": "primary corporate domain"},
            "description": {"type": "string", "description": "one line: who this company is"},
            "input": {"type": "string", "description": "the original input entry this company matches"},
        },
        "required": ["name", "domain"],
    }}},
    "required": ["customers"],
}


async def run_pipeline(domain: str, max_customers: int, emit, api_key: str | None = None,
                       custom_customers: str | None = None, signals: list[dict] | None = None):
    signals = signals or [dict(s) for s in DEFAULT_SIGNALS]
    exa = ExaClient(emit, api_key=api_key)
    t0 = time.monotonic()
    try:
        # ---- phase 0: does this domain even exist on the web? --------------
        # /answer will cheerfully invent a plausible company for a nonsense
        # domain, so verify Exa's index knows the site before profiling
        probe = await exa.post("/search", {
            "query": "company homepage", "includeDomains": [domain], "numResults": 3,
        }, "domain-probe")
        if exa.auth_error:
            await emit({"type": "error",
                        "message": "Exa rejected the API key (HTTP 401). If you added your own key, "
                                   "fix or clear it in the API key panel and try again."})
            return None
        probe_hits = [r for r in (probe or {}).get("results", [])
                      if result_domain(r.get("url", "")).endswith(domain)]
        if not probe_hits:
            await emit({"type": "error",
                        "message": f"Exa's index has no pages from '{domain}' — that domain doesn't "
                                   f"appear to be a live company site. Check the spelling, or try the "
                                   f"main corporate domain."})
            return None

        # ---- phase 1: vendor profile --------------------------------------
        await emit({"type": "phase", "phase": "profile",
                    "label": f"Profiling {domain} — what do they sell, who competes, what does churn look like?"})
        profile_payload = {
            "query": (f"What does the company at {domain} sell, who buys it, and who are "
                      f"its direct competitors? Focus on the product category and what a "
                      f"customer would have to build or buy instead if they stopped using it."),
            "outputSchema": PROFILE_SCHEMA,
        }
        prof_data = await exa.post("/answer", profile_payload, "vendor-profile")
        profile = parse_json_maybe((prof_data or {}).get("answer"))
        if not profile or not profile.get("company_name"):
            await emit({"type": "error",
                        "message": f"Couldn't build a profile for '{domain}'. Is it a real company domain? "
                                   f"Try the main corporate domain (e.g. 'stripe.com', not a docs subdomain)."})
            return None
        profile.setdefault("competitors", [])
        profile.setdefault("churn_modes", [])
        # /answer sometimes returns "Exa (formerly known as Exa.ai)" — the
        # parenthetical leaks into every lane query and status label
        vendor_name = re.sub(r"\s*\([^)]*\)", "", profile["company_name"]).strip()
        vendor_name = vendor_name or profile["company_name"]
        profile["company_name"] = vendor_name

        # ---- phase 1b: competitor top-up via findSimilar -------------------
        similar_payload = {
            "url": f"https://{domain}", "numResults": 8,
            "excludeSourceDomain": True, "category": "company",
            "contents": {"summary": {
                "query": (f"Is the company that owns this page a DIRECT competitor of "
                          f"{profile['company_name']} — i.e. do they sell "
                          f"{profile['product_category']}? Give their company name."),
                "schema": {"type": "object", "properties": {
                    "is_direct_competitor": {"type": "boolean"},
                    "company_name": {"type": "string"}},
                    "required": ["is_direct_competitor", "company_name"]},
            }},
        }
        sim = await exa.post("/findSimilar", similar_payload, "competitor-topup")
        vendor_token = re.sub(r"\W", "", vendor_name.lower())[:6]
        known = {c["domain"].lower().removeprefix("www.") for c in profile["competitors"] if c.get("domain")}
        # findSimilar returns pages, not vetted companies: drop socials/aggregators
        # (a LinkedIn company profile must not put linkedin.com in the sweep set)
        NOT_COMPETITORS = {"linkedin.com", "x.com", "twitter.com", "facebook.com",
                           "youtube.com", "wikipedia.org", "crunchbase.com", "github.com",
                           "medium.com", "reddit.com", "g2.com", "producthunt.com",
                           "ycombinator.com", "pitchbook.com", "getlatka.com"}
        for r in (sim or {}).get("results", []):
            d = result_domain(r.get("url", ""))
            # identity-cluster guard: skip the vendor's own alt domains/subpages
            if not d or vendor_token in d.replace("-", "") or d.endswith(domain) or d in known:
                continue
            if any(d == b or d.endswith("." + b) for b in NOT_COMPETITORS):
                continue
            vet = parse_json_maybe(r.get("summary")) or {}
            if not vet.get("is_direct_competitor"):
                continue  # similar-looking is not the same as competing
            nm = (vet.get("company_name") or "").strip()[:40] or d
            profile["competitors"].append({"name": nm, "domain": d, "via": "findSimilar"})
            known.add(d)
        competitor_domains = [c["domain"].removeprefix("www.") for c in profile["competitors"] if c.get("domain")][:10]
        # the exact requests behind the two panels — shown verbatim in the UI
        # so demo viewers see which endpoint/prompt produced each output
        api_calls = {
            "profile": {"endpoint": "/answer", "payload": profile_payload},
            "competitors": {"endpoint": "/findSimilar", "payload": similar_payload},
        }
        await emit({"type": "profile", "data": profile, "domain": domain, "api": api_calls})

        # ---- phase 2: the account list --------------------------------------
        customers, seen_domains, seen_names = [], set(), set()
        pinned = 0          # accounts the user typed — always swept, never dropped
        VALID_DOMAIN = re.compile(r"^[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}$")

        def add(name, cdomain, evidence, source):
            cdomain = (cdomain or "").lower().strip().removeprefix("https://").removeprefix("http://").removeprefix("www.").rstrip("/")
            key = re.sub(r"\W", "", (name or "").lower())
            if not name or not VALID_DOMAIN.match(cdomain):
                return
            # dedupe by name too — 'StackAI' must not appear as both
            # stackai.com and stackai.ai
            if cdomain == domain or cdomain in seen_domains or key in seen_names:
                return
            if vendor_token in key[:12]:
                return
            seen_domains.add(cdomain)
            seen_names.add(key)
            customers.append({"name": name.strip(), "domain": cdomain,
                              "evidence": (evidence or "").strip()[:200], "source": source})

        if custom_customers:
            # the CS team knows their book — resolve their list to canonical
            # names + domains (canonical casing matters: the evidence gate
            # matches 'HubSpot', not 'hubspot')
            entries = [e.strip() for e in custom_customers.split(",") if e.strip()][:20]
            await emit({"type": "phase", "phase": "customers",
                        "label": f"Resolving your list of {len(entries)} accounts…"})

            def norm(s):
                return re.sub(r"\W", "", (s or "").lower())

            def unmatched():
                keys = {k for k in seen_names | {norm(d) for d in seen_domains} if k}
                return [e for e in entries
                        if norm(e) and not any(norm(e) in k or k in norm(e) for k in keys)]

            async def resolve(batch, tag):
                data = await exa.post("/answer", {
                    "query": (f"Identify each of these {len(batch)} companies: {'; '.join(batch)}. "
                              f"Context: they are B2B companies, likely customers of {vendor_name} "
                              f"({profile['product_category']}). Entries may be misspelled, "
                              f"lowercase, or missing spaces — 'open router' means OpenRouter, "
                              f"'stackai' means StackAI. Return one object for EVERY entry: the "
                              f"commonly used BRAND name with canonical casing (e.g. 'Cursor', not "
                              f"'Anysphere, Inc.'), primary corporate domain, one line on who they "
                              f"are, and 'input' echoing the original entry. Only skip an entry if "
                              f"no real company plausibly matches it."),
                    "outputSchema": RESOLVE_SCHEMA,
                }, tag)
                ans = parse_json_maybe((data or {}).get("answer")) or {}
                for c in ans.get("customers", []):
                    add(c.get("name"), c.get("domain"),
                        c.get("description") or "From your list", "your list")

            await resolve(entries, "resolve-customer-list")
            if unmatched():                       # second chance for the stragglers
                await resolve(unmatched(), "resolve-retry")
            for e in unmatched():                 # last resort: keep domain-shaped entries verbatim
                tok = e.lower().removeprefix("https://").removeprefix("http://").removeprefix("www.").rstrip("/")
                if VALID_DOMAIN.match(tok):
                    add(tok, tok, "From your list", "your list")
            pinned = len(customers)
            still_missing = unmatched()
            if still_missing:
                await emit({"type": "warning",
                            "message": f"Couldn't identify: {', '.join(still_missing[:6])} — "
                                       f"sweeping the {pinned} that resolved" +
                                       (", plus discovered accounts to fill the rest."
                                        if pinned < max_customers else ".")})

        # top up with discovered customers whenever the typed list leaves room.
        # pinned accounts were added first and add() dedupes, so they always
        # survive both discovery and the slice below
        target = max(max_customers, pinned)   # an explicit list always sweeps in full
        if len(customers) < target:
            await emit({"type": "phase", "phase": "customers",
                        "label": (f"Finding {target - pinned} more of {vendor_name}'s documented "
                                  f"customers to fill the sweep…") if pinned else
                                 f"Mapping {vendor_name}'s publicly documented customer base…"})
            cust_answer, cust_site, cust_web = await asyncio.gather(
                exa.post("/answer", {
                    "query": (f"List 15-20 well-known companies that are publicly documented "
                              f"customers or users of {vendor_name} ({domain}). Only include "
                              f"companies with public evidence: case studies on {domain}, press "
                              f"coverage, engineering blog mentions, or official partnership "
                              f"announcements. For each give the company domain and how we know."),
                    "outputSchema": CUSTOMERS_SCHEMA,
                }, "customers-answer"),
                exa.post("/search", {
                    "query": f"{vendor_name} customer case study: how a company uses {vendor_name} in production",
                    "includeDomains": [domain],
                    "numResults": 20,
                    "contents": {"summary": CASE_STUDY_SUMMARY},
                }, "customers-casestudies"),
                exa.post("/search", {
                    "query": f"companies using {vendor_name} in production: named customers and what they built",
                    "excludeDomains": [domain],
                    "numResults": 6,
                    "contents": {"summary": web_mentions_summary(vendor_name)},
                }, "customers-web"),
            )
            ans = parse_json_maybe((cust_answer or {}).get("answer")) or {}
            for c in ans.get("customers", []):
                add(c.get("name"), c.get("domain"), c.get("evidence"), "web evidence")
            for r in (cust_site or {}).get("results", []):
                s = parse_json_maybe(r.get("summary")) or {}
                if s.get("customer_name"):
                    add(s["customer_name"], s.get("customer_domain"),
                        f"Official case study: {s.get('use_case', '')}", "vendor case study")
            for r in (cust_web or {}).get("results", []):
                s = parse_json_maybe(r.get("summary")) or {}
                for c in (s.get("customers") or [])[:6]:
                    add(c.get("name"), c.get("domain"), c.get("evidence"), "web evidence")
            customers = customers[:target]

        if not customers:
            await emit({"type": "error",
                        "message": (f"Couldn't resolve any of those entries to companies, and found no "
                                    f"documented customers for {vendor_name} either. Use company names "
                                    f"or domains, comma-separated.") if custom_customers else
                                   (f"No publicly documented customers found for {vendor_name}. That "
                                    f"usually means a very early company or one that keeps its customer "
                                    f"list private — try a vendor with public case studies, or type your "
                                    f"own account list.")})
            return None
        if len(customers) < 5 and len(customers) < target:
            await emit({"type": "warning",
                        "message": f"Only {len(customers)} accounts to sweep — thin public footprint; "
                                   f"verdicts below cover what's publicly visible."})
        await emit({"type": "customers", "data": customers, "pinned": pinned})

        # ---- phase 3: signal sweep -----------------------------------------
        lanes_for, lanes_meta = lane_definitions(profile, competitor_domains, signals)
        await emit({"type": "lanes", "data": lanes_meta})
        await emit({"type": "phase", "phase": "sweep",
                    "label": f"Sweeping {len(customers)} accounts × {len(lanes_meta)} signal lanes "
                             f"({len(customers) * len(lanes_meta)} semantic searches)…"})
        verdicts = await asyncio.gather(*[
            sweep_customer(exa, c, lanes_for, domain, emit) for c in customers
        ])

        summary = {
            "vendor": vendor_name, "domain": domain,
            "counts": {t: sum(1 for v in verdicts if v["tier"] == t)
                       for t in ("at_risk", "watch", "healthy")},
            "elapsed_s": round(time.monotonic() - t0, 1),
            "api_calls": exa.calls,
            "cost_usd": round(exa.cost, 3),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        await emit({"type": "done", "data": summary})
        return {"profile": profile, "domain": domain, "customers": customers,
                "verdicts": verdicts, "summary": summary, "api": api_calls,
                "lanes": lanes_meta}
    finally:
        await exa.close()


# ---------------------------------------------------------------- routes


@app.get("/")
async def index():
    return FileResponse(ROOT / "static" / "index.html")


_key_checks: dict[str, deque] = {}   # ip -> recent key-check timestamps


@app.post("/api/key/check")
async def key_check(request: Request):
    """Verify an Exa key with one minimal search call. The key is used for
    this request only — never stored or logged."""
    ip = request.headers.get("x-real-ip") or (request.client.host if request.client else "?")
    now = time.time()
    dq = _key_checks.setdefault(ip, deque())
    while dq and now - dq[0] > 3600:
        dq.popleft()
    if len(dq) >= 10:
        return JSONResponse({"ok": False, "error": "Too many key checks — try again later."})
    dq.append(now)
    try:
        body = await request.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    key = str(body.get("key", "")).strip()[:120]
    if not key:
        return JSONResponse({"ok": False, "error": "Paste a key first."})
    try:
        async with httpx.AsyncClient(timeout=12.0) as c:
            r = await c.post(f"{EXA_BASE}/search",
                             json={"query": "exa.ai", "numResults": 1},
                             headers={"x-api-key": key, "Content-Type": "application/json"})
    except httpx.HTTPError:
        return JSONResponse({"ok": False, "error": "Couldn't reach the Exa API — try again."})
    if r.status_code == 200:
        return JSONResponse({"ok": True})
    if r.status_code in (401, 403):
        return JSONResponse({"ok": False, "error": "Exa rejected this key (401)."})
    return JSONResponse({"ok": False, "error": f"Exa returned HTTP {r.status_code}."})


@app.get("/api/cached")
async def cached_runs():
    out = []
    for f in sorted(CACHE_DIR.glob("*.json")):
        try:
            s = json.loads(f.read_text()).get("summary", {})
            out.append({"domain": f.stem, "generated_at": s.get("generated_at"),
                        "counts": s.get("counts")})
        except (json.JSONDecodeError, OSError):
            continue
    return JSONResponse(out)


def sse(event: dict):
    return f"data: {json.dumps(event)}\n\n"


@app.get("/api/scan")
async def scan_get(request: Request, url: str, max_customers: int = 15, mode: str = "live",
                   customers: str = ""):
    # GET kept for deep links (?url=exa.ai&auto=cached); always default lanes
    return await scan_impl(request, url, max_customers, mode, customers, None)


@app.post("/api/scan")
async def scan_post(request: Request):
    try:
        body = await request.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    try:
        max_customers = int(body.get("max_customers", 15))
    except (TypeError, ValueError):
        max_customers = 15
    return await scan_impl(request, str(body.get("url", "")), max_customers,
                           str(body.get("mode", "live")), str(body.get("customers", "")),
                           body.get("signals"))


async def scan_impl(request: Request, url: str, max_customers: int, mode: str,
                    customers: str, signals_raw):
    domain = normalize_domain(url)
    custom_list = customers.strip()[:2000] or None
    signals, signals_default = parse_signals(signals_raw)

    user_key = (request.headers.get("x-exa-key") or "").strip()[:120] or None

    async def stream():
        if not EXA_API_KEY and not user_key:
            yield sse({"type": "error",
                       "message": "No Exa API key configured — add yours in the API key panel "
                                  "(top right), or set EXA_API_KEY on the server."})
            return
        if not domain:
            yield sse({"type": "error",
                       "message": f"'{url}' doesn't look like a domain. Try something like 'exa.ai' or 'https://stripe.com'."})
            return
        cache_file = CACHE_DIR / f"{domain}.json"

        if mode == "cached":
            if not cache_file.exists():
                yield sse({"type": "error", "message": f"No cached run for {domain} yet — run it live once first."})
                return
            run = json.loads(cache_file.read_text())
            default_meta = [{"id": s["id"], "label": s["label"], "short": s["short"],
                             "color": s["color"], "weight": s["weight"]} for s in DEFAULT_SIGNALS]
            yield sse({"type": "replay", "generated_at": run["summary"]["generated_at"]})
            yield sse({"type": "profile", "data": run["profile"], "domain": run["domain"],
                       "api": run.get("api")})
            yield sse({"type": "lanes", "data": run.get("lanes") or default_meta})
            yield sse({"type": "customers", "data": run["customers"]})
            for v in run["verdicts"]:
                yield sse({"type": "verdict", "data": v})
                await asyncio.sleep(0.06)   # let the board fill visibly
            yield sse({"type": "done", "data": run["summary"]})
            return

        client_ip = request.headers.get("x-real-ip") or (request.client.host if request.client else "?")
        refusal = live_scan_gate(client_ip, has_own_key=bool(user_key))
        if refusal:
            yield sse({"type": "error", "message": refusal})
            return

        queue: asyncio.Queue = asyncio.Queue()

        async def emit(ev):
            await queue.put(ev)

        global _active_scans
        _active_scans += 1
        task = asyncio.create_task(
            run_pipeline(domain, max(5, min(20, max_customers)), emit,
                         api_key=user_key, custom_customers=custom_list, signals=signals))
        run = None
        try:
            while True:
                if task.done() and queue.empty():
                    break
                try:
                    ev = await asyncio.wait_for(queue.get(), timeout=0.25)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        task.cancel()
                        return
                    continue
                yield sse(ev)
                if ev["type"] in ("done", "error"):
                    break
            run = await task
        except asyncio.CancelledError:
            task.cancel()
            return
        finally:
            _active_scans -= 1
        # custom-list and custom-lane runs are personal slices — don't let them
        # overwrite the cached full-discovery run for the domain
        if run and not custom_list and signals_default:
            cache_file.write_text(json.dumps(run, indent=1))

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", 8000)))
