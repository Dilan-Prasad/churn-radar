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


def live_scan_gate(ip: str):
    """Returns an error string if this live scan should be refused, else
    records the scan and returns None."""
    if _active_scans >= MAX_CONCURRENT_SCANS:
        return ("Two live scans are already running — give them a minute to finish, "
                "or use Replay cached for an instant (free) result.")
    now = time.time()
    while _scan_log and now - _scan_log[0] > 3600:
        _scan_log.popleft()
    per_ip = _scan_log_by_ip.setdefault(ip, deque())
    while per_ip and now - per_ip[0] > 3600:
        per_ip.popleft()
    if len(_scan_log) >= LIVE_SCANS_PER_HOUR or len(per_ip) >= LIVE_SCANS_PER_IP_PER_HOUR:
        return ("Hourly live-scan budget reached (each live sweep costs real API credits). "
                "Try Replay cached, or come back in a bit.")
    _scan_log.append(now)
    per_ip.append(now)
    return None


app = FastAPI(title="Churn Radar")

# ---------------------------------------------------------------- exa client


class ExaClient:
    """Thin async Exa client: rate-spaced, retrying, cost-accounting."""

    def __init__(self, emit):
        self.emit = emit                      # push events to the SSE stream
        self.sem = asyncio.Semaphore(MAX_CONCURRENCY)
        self.gate = asyncio.Lock()
        self.last_start = 0.0
        self.calls = 0
        self.cost = 0.0
        self.http = httpx.AsyncClient(
            base_url=EXA_BASE,
            headers={"x-api-key": EXA_API_KEY, "Content-Type": "application/json"},
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


def lane_definitions(profile: dict, competitor_domains: list[str]):
    """The five churn-signal lanes, phrased from the vendor profile so every
    query is specific to what THIS vendor sells."""
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

    def lanes_for(customer):
        n = customer["name"]
        return [
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

    return lanes_for


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


MAX_EVIDENCE_PER_LANE = {"hiring_to_replace": 1}   # hiring is noisy; default 2


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
    evidence, seen_urls = [], set()
    lanes = lanes_for(customer)
    results = await asyncio.gather(*[
        exa.post("/search", lane["payload"], f"{lane['id']}:{customer['name']}")
        for lane in lanes
    ])
    for lane, data in zip(lanes, results):
        if not data:
            continue
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
                evidence.append(item)
    # keep only the strongest evidence per lane so one noisy lane can't
    # dominate the verdict
    by_lane = {}
    for e in sorted(evidence, key=lambda x: -x["points"]):
        by_lane.setdefault(e["lane"], []).append(e)
    kept = [e for lane_id, lst in by_lane.items()
            for e in lst[:MAX_EVIDENCE_PER_LANE.get(lane_id, 2)]]
    score = round(sum(e["points"] for e in kept), 2)
    strongest = max((e["points"] for e in kept), default=0.0)
    verdict = {
        "customer": customer,
        "score": score,
        "tier": tier_for(score, strongest),
        "evidence": sorted(kept, key=lambda x: -x["points"]),
    }
    await emit({"type": "verdict", "data": verdict})
    return verdict


async def run_pipeline(domain: str, max_customers: int, emit):
    exa = ExaClient(emit)
    t0 = time.monotonic()
    try:
        # ---- phase 0: does this domain even exist on the web? --------------
        # /answer will cheerfully invent a plausible company for a nonsense
        # domain, so verify Exa's index knows the site before profiling
        probe = await exa.post("/search", {
            "query": "company homepage", "includeDomains": [domain], "numResults": 3,
        }, "domain-probe")
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
        prof_data = await exa.post("/answer", {
            "query": (f"What does the company at {domain} sell, who buys it, and who are "
                      f"its direct competitors? Focus on the product category and what a "
                      f"customer would have to build or buy instead if they stopped using it."),
            "outputSchema": PROFILE_SCHEMA,
        }, "vendor-profile")
        profile = parse_json_maybe((prof_data or {}).get("answer"))
        if not profile or not profile.get("company_name"):
            await emit({"type": "error",
                        "message": f"Couldn't build a profile for '{domain}'. Is it a real company domain? "
                                   f"Try the main corporate domain (e.g. 'stripe.com', not a docs subdomain)."})
            return None
        profile.setdefault("competitors", [])
        profile.setdefault("churn_modes", [])
        vendor_name = profile["company_name"]

        # ---- phase 1b: competitor top-up via findSimilar -------------------
        sim = await exa.post("/findSimilar", {
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
        }, "competitor-topup")
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
        await emit({"type": "profile", "data": profile, "domain": domain})

        # ---- phase 2: customer discovery -----------------------------------
        await emit({"type": "phase", "phase": "customers",
                    "label": f"Mapping {vendor_name}'s publicly documented customer base…"})
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
        customers, seen_domains, seen_names = [], set(), set()
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
        customers = customers[:max_customers]
        if not customers:
            await emit({"type": "error",
                        "message": f"No publicly documented customers found for {vendor_name}. "
                                   f"That usually means a very early company or one that keeps its "
                                   f"customer list private — try a vendor with public case studies."})
            return None
        if len(customers) < 5:
            await emit({"type": "warning",
                        "message": f"Only {len(customers)} documented customers found — thin public "
                                   f"footprint; verdicts below cover what's publicly visible."})
        await emit({"type": "customers", "data": customers})

        # ---- phase 3: signal sweep -----------------------------------------
        await emit({"type": "phase", "phase": "sweep",
                    "label": f"Sweeping {len(customers)} accounts × 5 churn-signal lanes "
                             f"({len(customers) * 5} semantic searches)…"})
        lanes_for = lane_definitions(profile, competitor_domains)
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
                "verdicts": verdicts, "summary": summary}
    finally:
        await exa.close()


# ---------------------------------------------------------------- routes


@app.get("/")
async def index():
    return FileResponse(ROOT / "static" / "index.html")


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
async def scan(request: Request, url: str, max_customers: int = 15, mode: str = "live"):
    domain = normalize_domain(url)

    async def stream():
        if not EXA_API_KEY:
            yield sse({"type": "error", "message": "EXA_API_KEY is not set. Add it to .env or the environment."})
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
            yield sse({"type": "replay", "generated_at": run["summary"]["generated_at"]})
            yield sse({"type": "profile", "data": run["profile"], "domain": run["domain"]})
            yield sse({"type": "customers", "data": run["customers"]})
            for v in run["verdicts"]:
                yield sse({"type": "verdict", "data": v})
                await asyncio.sleep(0.06)   # let the board fill visibly
            yield sse({"type": "done", "data": run["summary"]})
            return

        client_ip = request.headers.get("x-real-ip") or (request.client.host if request.client else "?")
        refusal = live_scan_gate(client_ip)
        if refusal:
            yield sse({"type": "error", "message": refusal})
            return

        queue: asyncio.Queue = asyncio.Queue()

        async def emit(ev):
            await queue.put(ev)

        global _active_scans
        _active_scans += 1
        task = asyncio.create_task(run_pipeline(domain, max(5, min(20, max_customers)), emit))
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
        if run:
            cache_file.write_text(json.dumps(run, indent=1))

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", 8000)))
