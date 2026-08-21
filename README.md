# Churn Radar 📡

**Point it at any B2B company's domain. It maps their publicly documented customers,
then sweeps the live web for account-specific churn signals — with receipts.**

Type `exa.ai` → in ~40 seconds you get a ranked account-risk board: which of Exa's
own customers are building in-house retrieval, showing up in competitors'
changelogs, getting acquired, or cutting spend — every verdict backed by dated,
clickable evidence.

**Live demo: [churn.salua.ai](https://churn.salua.ai)** · slides: `slides.html`

---

## Who it's for

**The end user:** a Head of Customer Success / RevOps lead at a B2B company
(the demo persona: Exa's own CS team).

**Their Tuesday morning today:** the top-50 account review. A CSM opens 20 tabs —
each account's engineering blog, careers page, LinkedIn, recent news — and skims
for trouble. That's ~45 minutes per account per month, it only covers the accounts
someone remembers to check, and the signals that actually precede churn (a
competitor's changelog quietly shipping an integration with your customer, an
engineering blog post about an in-house replacement, a job posting for the team
that will make you redundant) are exactly the ones nobody thinks to search for.

**The failure of normal tools:** keyword alerts can't express "this account is
building something that replaces us." Google Alerts on `"HubSpot" churn` returns
nothing useful — the real evidence never contains the word churn, and it never
mentions the vendor at all. HubSpot's post about their in-house vector platform
doesn't say "Exa"; Tavily's changelog entry for Devin doesn't say "leaving Exa."
Finding these requires **semantic** retrieval — searching by meaning, scoped by
domain and date — which is precisely what Exa sells.

---

## What it does (one input → one output)

```
input:  a vendor domain (exa.ai, vercel.com, …)
output: a live account-risk board — AT RISK / WATCH / HEALTHY per customer,
        each verdict carrying dated evidence links, streamed as it's found
```

### The pipeline — four Exa capabilities, each doing real work

| Step | Exa capability | Why this one |
|---|---|---|
| 1. Profile the vendor | `/answer` + `outputSchema` | Turns a bare domain into structured intel: product category, named competitors, and 3–5 concrete *churn modes* for this specific vendor. This is what makes every downstream query vendor-specific instead of generic. |
| 2. Vet the competitive set | `/findSimilar` + schema summary | Finds competitors the profile missed by pure semantic similarity to the vendor's homepage; each candidate is vetted by a structured summary ("is this a *direct* competitor?") before it joins the sweep. |
| 3. Map the customer base | `/answer` + `/search` (scoped to the vendor's site, schema summaries) | Cross-references publicly documented customers: official case studies on the vendor's own domain + press/engineering-blog evidence from the open web. Only documented customers make the board. |
| 4. Sweep 5 signal lanes per account | `/search` with semantic queries, `includeDomains`, `category`, date windows, `highlights` + schema `summary` in one call | The core. Five lanes, phrased from the step-1 profile (see below). |

### The five churn-signal lanes

Every lane is a *semantic* query generated from the vendor profile — this is why
the signals stay hyper-relevant to whatever vendor you type in:

1. **Competitor adoption** (weight 3) — the customer's name appearing on a
   competitor's product surface: changelogs, integration docs, case studies.
   Implemented as a semantic query scoped with `includeDomains` to the vetted
   competitor set. *(Real find: Tavily's changelog shipping a Devin/Cognition
   MCP integration; StackAI integration docs on tavily.com.)*
2. **Building in-house** (weight 3) — engineering blogs/talks about building the
   capability the vendor sells. The query uses the profile's `capability_phrase`,
   so for Exa it hunts for in-house search/retrieval infrastructure, for Vercel
   it hunts for in-house deployment platforms. *(Real find: HubSpot's
   "20-billion-vector Vector-as-a-Service" engineering post — which never
   mentions Exa, and no keyword alert on earth would have caught.)*
3. **Hiring to replace** (weight 2) — job postings whose responsibilities
   overlap the vendor's product. *(Real find: HubSpot's "MLOps, Retrieval
   Infra" role.)*
4. **Evaluating alternatives** (weight 2) — the customer benchmarking or
   comparing providers in the vendor's category.
5. **Budget / strategy risk** (weight 1.5) — layoffs, cost-cutting, acquisitions,
   pivots (news category, 8-month window). *(Real finds: Stripe acquiring
   OpenRouter; Asana acquiring StackAI; monday.com's 20% layoffs.)*

### How verdicts are scored — the model never decides alone

A design decision worth calling out: **signal type is determined by which lane
found the evidence, not by model judgment.** Each result then passes three
checks, split by what each layer is actually good at:

- **Extractive highlights are ground truth for relevance** — a result only counts
  if the customer is verifiably named in the page text (word-boundary,
  case-sensitive for brand names: `Cognition` the company ≠ "cognition" the noun).
- **Abstractive schema summaries classify** — strong / weak / none against a
  lane-specific rubric, plus a `mentions_vendor` counter-signal flag (a page that
  mentions the vendor may describe an integration *powered by* the vendor — those
  are down-weighted and flagged "verify" instead of counted as churn).
- **Deterministic code arbitrates** — lane weights × strength × recency decay,
  funding-news and customer-as-acquirer guards, per-lane evidence caps. Red
  requires at least one strong recent signal; a pile of weak ones can only reach
  WATCH. The scoring is fully auditable: every point on the board traces to a
  dated URL.

## Run it

```bash
pip install -r requirements.txt
echo "EXA_API_KEY=your-key" > .env
python app.py            # → http://localhost:8000
```

Type a domain, hit **Scan live**. Use the **accounts** slider (5–20) to size the
sweep — or paste your own book into the **customer list field** (comma-separated
names or domains, e.g. `HubSpot, monday.com, openrouter.ai`): the list is
resolved to canonical names + domains via `/answer` and auto-discovery is
skipped, which is how a real CS team would run it against their actual accounts.

Every full-discovery run is cached to `cache/<domain>.json` (custom-list runs
never overwrite it). `/?url=exa.ai&auto=live` deep-links a live scan;
`/?url=exa.ai&auto=cached` replays the cached run instantly with zero API calls
(demo insurance — clearly labeled as a replay).

The **under the hood** drawer (bottom of the page) streams every Exa API call
live — endpoint, lane, latency, cost — so you can narrate what the API is doing
while the board fills.

**Typical run** (15 accounts): ~50 Exa calls · ~40 s · **~$0.55**.

## Production notes / edge cases handled

- **Hallucination guard**: `/answer` will invent a plausible company for a
  nonsense domain — so a cheap `/search` scoped to the domain verifies the site
  exists in Exa's index before anything else runs.
- **`includeDomains` is a strong preference, not a hard constraint** (verified
  live: off-list results get backfilled when in-domain matches run thin) — the
  competitor lane re-enforces the scope client-side.
- **Rate limiting**: calls are spaced 0.13 s apart under a concurrency cap of 8
  (Exa's limit is 10 QPS), with backoff-retry on 429/5xx. A failed lane degrades
  that lane, never the run.
- **Empty states**: non-domain input, domain not in the index, zero documented
  customers, and thin (<5 customers) footprints each get their own explicit,
  human-readable path.
- **Public-deployment spend guard**: live scans are rate-limited (per-IP and
  global hourly budgets, max 2 concurrent) since each costs real API credits;
  cached replays are free and unthrottled. Tune via `LIVE_SCANS_PER_HOUR` /
  `LIVE_SCANS_PER_IP_PER_HOUR`.

## Deploying (how churn.salua.ai runs)

`deploy/churn-radar.service` (systemd, uvicorn on 127.0.0.1:8010) +
`deploy/nginx-churn.salua.ai.conf` (nginx reverse proxy with SSE buffering
disabled) + `certbot --nginx -d churn.salua.ai` for TLS. Install commands are
in each file's header comment.
- **Noise filters learned from live calibration**: funding rounds are growth
  (not "budget distress"), a customer *acquiring* someone is not "being
  acquired", ubiquitous-brand tutorial pages ("deploy a Stripe webhook on
  Render") are not competitor adoption, news articles hosted on a competitor's
  news property (for.you.com) are not competitor adoption.

## Q&A anchors

**Why Exa and not a keyword search API or scraping?**
Three of the five lanes are unanswerable by keyword: "engineering post about
building in-house what this vendor sells" has no reliable keyword form — the
best evidence (HubSpot's VaaS post) never names the vendor or the category.
And the highest-precision lane (customer named on a competitor's product
surface) needs semantic search *scoped by domain* — `includeDomains` over a
competitor set discovered by `findSimilar`. Scraping 10 competitor sites ×
every customer name is a crawler project with none of Exa's freshness or
ranking; here it's one API call per account.

**How does this scale 10×?**
The sweep is embarrassingly parallel per account-lane pair. 10× accounts =
linear cost (~$0.04/account) and the same wall-clock under a higher QPS tier.
The natural production shape is a nightly sweep of the whole book with
day-over-day *diffing* — surface only new signals, push to Slack/CRM.

**ROI story for the economic buyer?**
A CS team covering 200 accounts at 45 min/account/month is ~19 person-weeks a
year of manual checking that still misses the quiet signals. One sweep of 200
accounts costs about $8 in API calls and runs while the CSM gets coffee. One
saved mid-market account (say $30–60k ARR) pays for years of sweeps; catching
one Stripe-acquires-OpenRouter-class event a quarter early changes the renewal
conversation entirely.

**What would you build next sprint?**
(1) Nightly diff mode + Slack digest ("2 new signals across your book").
(2) CRM enrichment via webhook (risk tier as a Salesforce field).
(3) Expansion radar — the same lanes inverted find *growth* signals (the
`mentions_vendor` counter-signals are already collected).
(4) Websets for the customer-discovery step, replacing the two-source
cross-reference with a single filtered, enriched company list.

---

*Built for the Exa FDE take-home. Single-file FastAPI backend (`app.py`),
vanilla-JS frontend (`static/index.html`), no framework, no database — the
workflow logic is the product.*
