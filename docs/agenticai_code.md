# Daily Industry News Summarizer Agent - Source Code

Complete source of `agenticai.py`, a single-file agentic AI application built with **LangGraph**, **FastAPI**, **SQLite** and the **Anthropic Claude API** (optional).

## Run it

```powershell
pip install fastapi uvicorn feedparser langgraph anthropic
$env:ANTHROPIC_API_KEY = "sk-ant-..."   # optional - without it the rule-based summarizer is used
python agenticai.py              # web dashboard at http://127.0.0.1:8000
python agenticai.py --once       # one headless digest, printed as markdown
python agenticai.py --port 8080  # choose a port
```

## Structure of the file

| Section | What it contains |
|---|---|
| Configuration | Default feeds, reader profile, keywords and thresholds |
| Storage | SQLite tables `settings`, `seen`, `digests` |
| Helpers | Text cleaning, article IDs, Jaccard similarity, headline-echo detection, topic labels, feed fetching |
| Claude layer | `ClaudeClient` with structured output and graceful fallbacks; rule-based extractive summarizer |
| LangGraph pipeline | 7 nodes: fetch, dedupe, prefilter, summarize, rank, brief, persist, with per-node progress tracking |
| Rendering and delivery | Markdown / Slack / HTML email renderers, Slack and SMTP senders |
| Run orchestration | Background run, live status, daily scheduler |
| Web app | FastAPI routes and the single-page dashboard (HTML, CSS, JavaScript) |
| Entrypoint | CLI arguments, `--once` mode, server start-up |

## Full source

````python
#!/usr/bin/env python3
"""
================================================================================
 DAILY INDUSTRY NEWS SUMMARIZER AGENT  --  single-file LangGraph application
================================================================================

 What it does
 ------------
 Aggregates articles from RSS feeds, filters them for relevance to YOUR
 industry profile, and writes a concise 3-bullet summary for each one, plus a
 cross-cutting executive brief. Serves everything on a local web dashboard and
 can push the digest to Slack or email.

 Quick start
 -----------
   1. pip install fastapi uvicorn feedparser langgraph anthropic
   2. export ANTHROPIC_API_KEY=sk-ant-...        (Windows: set ANTHROPIC_API_KEY=...)
   3. python news_agent.py
   4. Your browser opens http://127.0.0.1:8000  -- click "Run digest now".

 Without an API key it still runs: it falls back to an extractive (non-LLM)
 summarizer so you can see the whole pipeline work, then add the key later.

 Optional environment variables
 ------------------------------
   ANTHROPIC_API_KEY    Claude API key (https://platform.claude.com)
   NEWS_AGENT_MODEL     default "claude-opus-5-5"
   NEWS_AGENT_PORT      default 8000
   DAILY_RUN_AT         e.g. "07:30" -> runs itself once a day at that local time
   SLACK_WEBHOOK_URL    Slack incoming webhook, for the "Send to Slack" button
   SMTP_HOST SMTP_PORT SMTP_USER SMTP_PASS EMAIL_FROM EMAIL_TO   for email delivery

 CLI
 ---
   python news_agent.py              # web dashboard (default)
   python news_agent.py --once       # run one digest headless, print markdown, deliver
   python news_agent.py --port 8080  # pick a port
================================================================================
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import os
import re
import smtplib
import socket
import sqlite3
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from html import escape as html_escape
from html import unescape as html_unescape
from typing import Any, Dict, List, Optional, Tuple, TypedDict

# --------------------------------------------------------------------------- #
# Dependency check with a friendly message instead of a raw traceback
# --------------------------------------------------------------------------- #
_MISSING: List[str] = []
try:
    import feedparser  # type: ignore
except ImportError:
    _MISSING.append("feedparser")
try:
    from fastapi import FastAPI, HTTPException  # type: ignore
    from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse  # type: ignore
except ImportError:
    _MISSING.append("fastapi")
try:
    import uvicorn  # type: ignore
except ImportError:
    _MISSING.append("uvicorn")
try:
    from langgraph.graph import END, StateGraph  # type: ignore
except ImportError:
    _MISSING.append("langgraph")

if _MISSING:
    sys.stderr.write(
        "\n  Missing packages: %s\n\n  Install them with:\n\n"
        "      pip install fastapi uvicorn feedparser langgraph anthropic\n\n"
        % ", ".join(_MISSING)
    )
    sys.exit(1)

# anthropic is optional -- the app degrades to an extractive summarizer without it
try:
    import anthropic  # type: ignore
except ImportError:
    anthropic = None  # type: ignore

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "news_agent.db")
USER_AGENT = "DailyNewsDigestAgent/1.0 (+local RSS reader)"
MODEL = os.getenv("NEWS_AGENT_MODEL", "claude-opus-5-5")

# Some corporate networks / proxied containers put their CA bundle in an env var.
_CA_FILE = os.getenv("SSL_CERT_FILE") or os.getenv("REQUESTS_CA_BUNDLE")
_SSL_CTX = ssl.create_default_context(cafile=_CA_FILE) if _CA_FILE else ssl.create_default_context()

# --------------------------------------------------------------------------- #
# Default configuration (editable live in the dashboard, stored in SQLite)
# --------------------------------------------------------------------------- #
DEFAULT_FEEDS = [
    "https://techcrunch.com/feed/",
    "https://www.theverge.com/rss/index.xml",
    "https://feeds.arstechnica.com/arstechnica/technology-lab",
    "https://www.technologyreview.com/feed/",
    "https://venturebeat.com/category/ai/feed/",
    "https://www.wired.com/feed/category/business/latest/rss",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://hnrss.org/frontpage?points=150",
    "https://www.zdnet.com/news/rss.xml",
    "https://feeds.feedburner.com/oreilly/radar",
]

DEFAULT_SETTINGS: Dict[str, Any] = {
    # Who the digest is for. This is the single biggest lever on relevance.
    "profile": (
        "Executive and research team at a mid-size enterprise software company. "
        "We care about AI/ML platform shifts, model releases and pricing, cloud "
        "infrastructure, enterprise SaaS competition, funding and M&A in our space, "
        "data privacy and AI regulation, and security incidents that affect "
        "enterprise buyers."
    ),
    "keywords": [
        "artificial intelligence", "AI", "LLM", "machine learning", "foundation model",
        "OpenAI", "Anthropic", "Google DeepMind", "GPU", "data center",
        "enterprise software", "SaaS", "cloud", "funding round", "acquisition",
        "IPO", "regulation", "privacy", "data breach", "cybersecurity", "chips",
    ],
    "exclude": ["celebrity", "sports scores", "horoscope", "recipe", "coupon", "deal of the day"],
    "feeds": DEFAULT_FEEDS,
    "lookback_hours": 36,
    "max_articles": 10,
    "min_relevance": 55,
    "use_google_news": True,
    "model": MODEL,
}

# --------------------------------------------------------------------------- #
# Storage
# --------------------------------------------------------------------------- #
_DB_LOCK = threading.Lock()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _DB_LOCK, db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS seen (
                article_id TEXT PRIMARY KEY,
                title      TEXT,
                link       TEXT,
                first_seen TEXT
            );
            CREATE TABLE IF NOT EXISTS digests (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at   TEXT NOT NULL,
                article_count INTEGER NOT NULL,
                payload      TEXT NOT NULL
            );
            """
        )


def load_settings() -> Dict[str, Any]:
    out = dict(DEFAULT_SETTINGS)
    with _DB_LOCK, db() as conn:
        for row in conn.execute("SELECT key, value FROM settings"):
            try:
                out[row["key"]] = json.loads(row["value"])
            except json.JSONDecodeError:
                pass
    return out


def save_settings(patch: Dict[str, Any]) -> Dict[str, Any]:
    allowed = set(DEFAULT_SETTINGS)
    with _DB_LOCK, db() as conn:
        for key, value in patch.items():
            if key in allowed:
                conn.execute(
                    "INSERT INTO settings(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, json.dumps(value)),
                )
    return load_settings()


def mark_seen(articles: List[Dict[str, Any]]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _DB_LOCK, db() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO seen(article_id, title, link, first_seen) VALUES(?,?,?,?)",
            [(a["id"], a["title"], a["link"], now) for a in articles],
        )
        # keep the dedupe table from growing forever
        conn.execute(
            "DELETE FROM seen WHERE first_seen < ?",
            ((datetime.now(timezone.utc) - timedelta(days=45)).isoformat(),),
        )


def already_seen(ids: List[str]) -> set:
    if not ids:
        return set()
    out = set()
    with _DB_LOCK, db() as conn:
        for chunk_start in range(0, len(ids), 400):
            chunk = ids[chunk_start:chunk_start + 400]
            placeholders = ",".join("?" * len(chunk))
            rows = conn.execute(
                f"SELECT article_id FROM seen WHERE article_id IN ({placeholders})", chunk
            )
            out.update(r["article_id"] for r in rows)
    return out


def store_digest(digest: Dict[str, Any]) -> int:
    with _DB_LOCK, db() as conn:
        cur = conn.execute(
            "INSERT INTO digests(created_at, article_count, payload) VALUES(?,?,?)",
            (digest["created_at"], len(digest.get("articles", [])), json.dumps(digest)),
        )
        return int(cur.lastrowid)


def get_digest(digest_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    with _DB_LOCK, db() as conn:
        if digest_id is None:
            row = conn.execute("SELECT * FROM digests ORDER BY id DESC LIMIT 1").fetchone()
        else:
            row = conn.execute("SELECT * FROM digests WHERE id=?", (digest_id,)).fetchone()
    if not row:
        return None
    payload = json.loads(row["payload"])
    payload["id"] = row["id"]
    return payload


def list_digests(limit: int = 30) -> List[Dict[str, Any]]:
    with _DB_LOCK, db() as conn:
        rows = conn.execute(
            "SELECT id, created_at, article_count FROM digests ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "for", "on", "with", "is", "are",
    "as", "at", "by", "from", "that", "this", "it", "its", "be", "will", "has", "have",
    "new", "says", "after", "how", "why", "what", "can", "but", "not", "you", "your",
}


def clean_text(raw: str, limit: int = 1400) -> str:
    if not raw:
        return ""
    text = html_unescape(_TAG_RE.sub(" ", raw))
    text = _WS_RE.sub(" ", text).strip()
    return text[:limit]


def article_id(link: str, title: str) -> str:
    base = (link or "").split("?")[0].strip().lower() or (title or "").strip().lower()
    return hashlib.sha1(base.encode("utf-8", "replace")).hexdigest()[:16]


def title_tokens(title: str) -> set:
    words = re.findall(r"[a-z0-9]+", (title or "").lower())
    return {w for w in words if w not in _STOPWORDS and len(w) > 2}


def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


_PUBLISHER_RE = re.compile(r"\s[-–—|]\s([^-–—|]{2,40})$")
_ACRONYMS = {"ai", "llm", "gpu", "cpu", "saas", "ipo", "api", "ml"}


def is_headline_echo(title: str, excerpt: str) -> bool:
    """True when a feed 'summary' is just the headline again (typical of Google News)."""
    words = title_tokens(excerpt)
    if not words:
        return True
    return len(words & title_tokens(title)) / len(words) >= 0.75


def topic_label(keyword: str) -> str:
    """Display form of a matched keyword: AI, LLM, OpenAI, Machine Learning."""
    parts = []
    for word in keyword.split():
        if word.lower() in _ACRONYMS:
            parts.append(word.upper())
        elif any(ch.isupper() for ch in word):
            parts.append(word)
        else:
            parts.append(word.capitalize())
    return " ".join(parts) or "General"


def clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0].rstrip(",;:") + "..."


def human_age(iso_ts: str) -> str:
    try:
        then = datetime.fromisoformat(iso_ts)
    except (ValueError, TypeError):
        return ""
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    mins = max(0, int((datetime.now(timezone.utc) - then).total_seconds() // 60))
    if mins < 60:
        return f"{mins}m ago"
    if mins < 60 * 48:
        return f"{mins // 60}h ago"
    return f"{mins // 1440}d ago"


def fetch_url(url: str, timeout: int = 20) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
        return resp.read()


def google_news_feed(keywords: List[str]) -> Optional[str]:
    """Build a Google News RSS search feed from the top keywords."""
    terms = [k.strip() for k in keywords if k.strip()][:6]
    if not terms:
        return None
    query = " OR ".join(f'"{t}"' for t in terms) + " when:2d"
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode(
        {"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"}
    )


# =========================================================================== #
#  CLAUDE LAYER
#  Uses the official Anthropic SDK with structured outputs (JSON schema) so
#  every summary comes back in exactly the shape we need. Degrades gracefully:
#  older SDKs / unsupported params fall back to plain text + JSON extraction,
#  and with no API key at all we use a local extractive summarizer.
# =========================================================================== #

ARTICLE_SCHEMA = {
    "type": "object",
    "properties": {
        "is_relevant": {"type": "boolean"},
        "relevance": {"type": "integer"},
        "topic": {"type": "string"},
        "bullets": {"type": "array", "items": {"type": "string"}},
        "why_it_matters": {"type": "string"},
    },
    "required": ["is_relevant", "relevance", "topic", "bullets", "why_it_matters"],
    "additionalProperties": False,
}

BRIEF_SCHEMA = {
    "type": "object",
    "properties": {
        "headline": {"type": "string"},
        "themes": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["headline", "themes"],
    "additionalProperties": False,
}

ARTICLE_SYSTEM = (
    "You are a news analyst for a busy executive team. You read one article at a "
    "time and decide whether it matters to this specific reader, then compress it.\n"
    "Rules:\n"
    "- Score relevance 0-100 against the reader profile. Be strict: generic consumer "
    "news, listicles, product-deal posts and celebrity items score under 30.\n"
    "- Write EXACTLY 3 bullets. Each bullet is one sentence, under 25 words, and "
    "states a concrete fact (who, what, how much, when) -- never 'the article discusses'.\n"
    "- Use only information present in the title and excerpt. If the excerpt is thin, "
    "say what is known and do not invent numbers, names or outcomes.\n"
    "- 'why_it_matters' is one sentence written for this reader's decisions.\n"
    "- 'topic' is a 1-3 word category such as 'AI models', 'Funding', 'Regulation'."
)

BRIEF_SYSTEM = (
    "You write the opening brief of a daily industry digest for executives. "
    "Given today's selected stories, identify the cross-cutting themes. "
    "Return a one-line headline (under 14 words) and 3-5 theme bullets; each bullet "
    "ties together at least two stories or states the single most consequential "
    "development, in under 28 words. No preamble, no hedging."
)


def _extract_json(text: str) -> Optional[dict]:
    """Best-effort JSON extraction for responses that are not schema-constrained."""
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    return None


class ClaudeClient:
    """Thin wrapper over the Anthropic Messages API with capability fallbacks."""

    def __init__(self, model: str = MODEL, log=print):
        self.model = model
        self.log = log
        self.client = None
        self.reason = ""
        self._structured = True          # output_config.format support
        self._beta_fallbacks = True      # server-side refusal fallback support
        self._lock = threading.Lock()

        if anthropic is None:
            self.reason = "the 'anthropic' package is not installed"
            return
        if not (os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_AUTH_TOKEN")):
            self.reason = "ANTHROPIC_API_KEY is not set"
            return
        try:
            self.client = anthropic.Anthropic(max_retries=3, timeout=120.0)
        except Exception as exc:  # pragma: no cover - construction rarely fails
            self.reason = f"could not create the Anthropic client: {exc}"

    @property
    def available(self) -> bool:
        return self.client is not None

    # -- low level ---------------------------------------------------------- #
    def _create(self, **kwargs):
        """Call the API, stepping down through optional parameters if rejected."""
        if self._beta_fallbacks:
            try:
                # Safety classifiers can decline an occasional article (security /
                # bio / cyber news). Server-side fallbacks keep the run going by
                # routing that one request to another model instead of failing.
                return self.client.beta.messages.create(
                    betas=["server-side-fallback-2026-07-01"],
                    fallbacks="default",
                    **kwargs,
                )
            except TypeError:
                with self._lock:
                    self._beta_fallbacks = False
            except Exception as exc:
                if self._is_param_error(exc):
                    with self._lock:
                        self._beta_fallbacks = False
                else:
                    raise
        return self.client.messages.create(**kwargs)

    @staticmethod
    def _is_param_error(exc: Exception) -> bool:
        status = getattr(exc, "status_code", None)
        if status not in (400, 404):
            return False
        blob = str(exc).lower()
        return any(w in blob for w in ("fallback", "beta", "unexpected", "output_config", "effort"))

    def json_call(
        self,
        system: str,
        user: str,
        schema: dict,
        effort: str = "low",
        max_tokens: int = 4096,
    ) -> Optional[dict]:
        if not self.available:
            return None
        attempts: List[Dict[str, Any]] = []
        if self._structured:
            attempts.append({"output_config": {"format": {"type": "json_schema", "schema": schema},
                                               "effort": effort}})
            attempts.append({"output_config": {"format": {"type": "json_schema", "schema": schema}}})
        attempts.append({})  # last resort: plain text, parse the JSON ourselves

        for index, extra in enumerate(attempts):
            prompt = user if extra else user + "\n\nReply with JSON only, no prose or code fences."
            try:
                response = self._create(
                    model=self.model,
                    max_tokens=max_tokens,
                    system=system,
                    messages=[{"role": "user", "content": prompt}],
                    **extra,
                )
            except Exception as exc:
                if self._is_param_error(exc) and index < len(attempts) - 1:
                    if "output_config" in extra:
                        with self._lock:
                            self._structured = False
                    continue
                self.log(f"   ! Claude call failed: {type(exc).__name__}: {str(exc)[:180]}")
                return None

            # A safety classifier may decline; that is an HTTP 200, not an exception.
            if getattr(response, "stop_reason", None) == "refusal":
                details = getattr(response, "stop_details", None)
                self.log(f"   ! model declined this item ({getattr(details, 'category', 'unknown')})")
                return None

            text = "".join(
                block.text for block in response.content
                if getattr(block, "type", "") == "text"
            ).strip()
            data = _extract_json(text)
            if data is not None:
                return data
            if index == len(attempts) - 1:
                self.log("   ! could not parse a JSON response")
        return None


# --------------------------------------------------------------------------- #
# Non-LLM fallback summarizer (so the app is useful before you add a key)
# --------------------------------------------------------------------------- #
def extractive_summary(article: Dict[str, Any]) -> Dict[str, Any]:
    """Rule-based summary used when no Claude access is available.

    Uses real sentences from the feed excerpt when there are any, then adds
    honest context lines (publisher, age, matched interests) instead of padding
    with the headline. Relevance = 40 + 6 per keyword point + up to 8 for freshness.
    """
    matched = article.get("matched") or []
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", article.get("excerpt") or "")
                 if len(s.strip()) > 30]
    bullets = [clip(s, 200) for s in sentences[:3]]
    if len(bullets) < 3:
        bullets.append(f"Reported by {article['source']}, {human_age(article['published'])}.")
    if len(bullets) < 3 and matched:
        bullets.append("Matches your interests: " + ", ".join(matched[:4]) + ".")

    try:
        age_hours = (datetime.now(timezone.utc)
                     - datetime.fromisoformat(article["published"])).total_seconds() / 3600
    except (KeyError, ValueError):
        age_hours = 36.0
    freshness = round(max(0.0, 24 - age_hours) / 24 * 8)

    why = ("Matches your keywords: " + ", ".join(matched[:4]) + ". ") if matched else ""
    return {
        "is_relevant": True,
        "relevance": min(95, 40 + article.get("keyword_score", 0) * 6 + freshness),
        "topic": topic_label(max(matched, key=len)) if matched else "General",
        "bullets": bullets,
        "why_it_matters": why + "Scored by keyword rules (no AI model used).",
        "engine": "extractive",
    }


# =========================================================================== #
#  LANGGRAPH PIPELINE
#  fetch -> dedupe -> prefilter -> summarize -> rank -> brief -> persist
# =========================================================================== #

class AgentState(TypedDict, total=False):
    settings: Dict[str, Any]
    articles: List[Dict[str, Any]]      # everything pulled from the feeds
    duplicates: List[Dict[str, Any]]    # near-duplicate headlines, remembered for dedupe
    candidates: List[Dict[str, Any]]    # survived dedupe + keyword prefilter
    summarized: List[Dict[str, Any]]    # got a summary
    selected: List[Dict[str, Any]]      # above the relevance threshold, ranked
    headline: str
    themes: List[str]
    stats: Dict[str, Any]
    errors: List[str]
    digest: Dict[str, Any]


class Runner:
    """Holds the per-run log sink and the Claude client for the graph nodes."""

    def __init__(self, log=print):
        self.log = log
        self.llm = ClaudeClient(model=load_settings().get("model", MODEL), log=log)


# -- node 1: fetch ---------------------------------------------------------- #
def node_fetch(state: AgentState, run: "Runner") -> AgentState:
    settings = state["settings"]
    feeds = [f.strip() for f in settings.get("feeds", []) if f.strip()]
    if settings.get("use_google_news"):
        extra = google_news_feed(settings.get("keywords", []))
        if extra:
            feeds.append(extra)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=int(settings.get("lookback_hours", 36)))
    run.log(f"Fetching {len(feeds)} feeds (articles newer than "
            f"{settings.get('lookback_hours', 36)}h)...")

    errors: List[str] = []
    articles: List[Dict[str, Any]] = []
    lock = threading.Lock()

    def pull(url: str) -> None:
        label = urllib.parse.urlparse(url).netloc or url
        try:
            raw = fetch_url(url)
        except Exception as exc:
            with lock:
                errors.append(f"{label}: {type(exc).__name__}")
            run.log(f"   x {label}: {type(exc).__name__}")
            return
        parsed = feedparser.parse(raw)
        source = (parsed.feed.get("title") or label).strip()[:60]
        is_aggregator = "news.google.com" in url
        kept = 0
        for entry in parsed.entries:
            link = (entry.get("link") or "").strip()
            title = clean_text(entry.get("title") or "", 300)
            if not title:
                continue
            # Aggregators (Google News) put the real publisher in <source> or as a
            # " - Publisher" suffix; show that instead of the search-feed name.
            publisher = ""
            origin = entry.get("source")
            if isinstance(origin, dict):
                publisher = clean_text(origin.get("title") or "", 60)
            if is_aggregator:
                if not publisher:
                    found = _PUBLISHER_RE.search(title)
                    publisher = found.group(1).strip() if found else ""
                if publisher and title.endswith(publisher):
                    title = re.sub(r"\s*[-–—|]\s*" + re.escape(publisher) + r"\s*$", "", title) or title
            stamp = entry.get("published_parsed") or entry.get("updated_parsed")
            published = (
                datetime.fromtimestamp(calendar.timegm(stamp), tz=timezone.utc)
                if stamp else datetime.now(timezone.utc)
            )
            if published < cutoff:
                continue
            excerpt = clean_text(
                entry.get("summary") or
                (entry.get("content", [{}])[0].get("value") if entry.get("content") else "")
            )
            if excerpt and is_headline_echo(title, excerpt):
                excerpt = ""
            with lock:
                articles.append({
                    "id": article_id(link, title),
                    "title": title,
                    "link": link,
                    "source": publisher or source,
                    "published": published.isoformat(),
                    "excerpt": excerpt,
                })
            kept += 1
        run.log(f"   + {source}: {kept} recent item(s)")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(pull, feeds))

    articles.sort(key=lambda a: a["published"], reverse=True)
    run.log(f"Fetched {len(articles)} articles in the window.")
    return {"articles": articles, "errors": errors,
            "stats": {"fetched": len(articles), "feeds": len(feeds)}}


# -- node 2: dedupe --------------------------------------------------------- #
def node_dedupe(state: AgentState, run: "Runner") -> AgentState:
    articles = state.get("articles", [])
    seen_ids = already_seen([a["id"] for a in articles])

    unique: List[Dict[str, Any]] = []
    duplicates: List[Dict[str, Any]] = []
    fingerprints: List[set] = []
    dropped_old = 0

    for article in articles:
        if article["id"] in seen_ids:
            dropped_old += 1
            continue
        tokens = title_tokens(article["title"])
        if any(jaccard(tokens, other) > 0.62 for other in fingerprints):
            duplicates.append(article)   # remembered so it is not "new" tomorrow
            continue
        fingerprints.append(tokens)
        unique.append(article)
    dropped_dup = len(duplicates)

    run.log(f"Dedupe: dropped {dropped_old} already-digested and "
            f"{dropped_dup} near-duplicate headline(s) -> {len(unique)} left.")
    stats = dict(state.get("stats", {}))
    stats.update({"already_seen": dropped_old, "duplicates": dropped_dup, "unique": len(unique)})
    return {"articles": unique, "duplicates": duplicates, "stats": stats}


# -- node 3: keyword prefilter (cheap, before we spend tokens) -------------- #
def node_prefilter(state: AgentState, run: "Runner") -> AgentState:
    settings = state["settings"]
    # whole-word matching, so the keyword "AI" does not fire on "said" or "again"
    keywords = [
        (k.strip(), re.compile(r"(?<![a-z0-9])" + re.escape(k.strip().lower()) + r"(?![a-z0-9])"))
        for k in settings.get("keywords", []) if k.strip()
    ]
    excludes = [e.strip().lower() for e in settings.get("exclude", []) if e.strip()]
    max_articles = int(settings.get("max_articles", 10))
    budget = min(28, max(max_articles * 2, 12))   # how many articles we send to the LLM

    scored: List[Dict[str, Any]] = []
    for article in state.get("articles", []):
        title = article["title"].lower()
        body = article["excerpt"].lower()
        if any(ex in title or ex in body for ex in excludes):
            continue
        score, matched = 0, []
        for keyword, pattern in keywords:
            if pattern.search(title):
                score += 3
                matched.append(keyword)
            elif pattern.search(body):
                score += 1
                matched.append(keyword)
        age_hours = max(0.0, (datetime.now(timezone.utc)
                              - datetime.fromisoformat(article["published"])).total_seconds() / 3600)
        article = dict(article, keyword_score=score, matched=matched[:6],
                       rank_hint=score * 10 + max(0.0, 36 - age_hours) / 6)
        scored.append(article)

    scored.sort(key=lambda a: a["rank_hint"], reverse=True)
    hits = [a for a in scored if a["keyword_score"] > 0]
    candidates = hits[:budget]
    if len(candidates) < min(budget, 8):   # top up with the freshest unmatched items
        extras = [a for a in scored if a["keyword_score"] == 0][: budget - len(candidates)]
        candidates += extras

    run.log(f"Prefilter: {len(hits)} keyword match(es); sending {len(candidates)} "
            f"article(s) to the summarizer.")
    stats = dict(state.get("stats", {}))
    stats.update({"keyword_hits": len(hits), "to_summarize": len(candidates)})
    return {"candidates": candidates, "stats": stats}


# -- node 4: summarize (3 bullets each, in parallel) ------------------------ #
def node_summarize(state: AgentState, run: "Runner") -> AgentState:
    settings = state["settings"]
    candidates = state.get("candidates", [])
    if not candidates:
        return {"summarized": []}

    if run.llm.available:
        run.log(f"Summarizing {len(candidates)} article(s) with {run.llm.model}...")
    else:
        run.log(f"No Claude access ({run.llm.reason}) -- using the extractive fallback.")

    profile = settings.get("profile", "")
    counter = {"done": 0}
    lock = threading.Lock()

    def summarize_one(article: Dict[str, Any]) -> Dict[str, Any]:
        result: Optional[Dict[str, Any]] = None
        if run.llm.available:
            user = (
                f"READER PROFILE\n{profile}\n\n"
                f"ARTICLE\nTitle: {article['title']}\n"
                f"Source: {article['source']}\n"
                f"Published: {human_age(article['published'])}\n"
                f"Excerpt: {article['excerpt'] or '(no excerpt supplied by the feed)'}\n"
                f"URL: {article['link']}"
            )
            result = run.llm.json_call(ARTICLE_SYSTEM, user, ARTICLE_SCHEMA,
                                       effort="low", max_tokens=4096)
        if not result:
            result = extractive_summary(article)
        else:
            result["engine"] = run.llm.model

        bullets = [str(b).strip() for b in (result.get("bullets") or []) if str(b).strip()][:3]
        if not bullets:
            bullets = [article["title"]]
        try:
            relevance = max(0, min(100, int(float(result.get("relevance", 0)))))
        except (TypeError, ValueError):
            relevance = 0

        with lock:
            counter["done"] += 1
            _set_progress("summarize", counter["done"], len(candidates))
            if counter["done"] % 4 == 0 or counter["done"] == len(candidates):
                run.log(f"   ... {counter['done']}/{len(candidates)} summarized")

        return dict(
            article,
            age=human_age(article["published"]),
            bullets=bullets,
            relevance=relevance,
            topic=str(result.get("topic") or "General")[:40],
            why_it_matters=str(result.get("why_it_matters") or "")[:400],
            is_relevant=bool(result.get("is_relevant", True)),
            engine=result.get("engine", "unknown"),
        )

    workers = 6 if run.llm.available else 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        summarized = list(pool.map(summarize_one, candidates))
    return {"summarized": summarized}


# -- node 5: rank and cut --------------------------------------------------- #
def node_rank(state: AgentState, run: "Runner") -> AgentState:
    settings = state["settings"]
    threshold = int(settings.get("min_relevance", 55))
    max_articles = int(settings.get("max_articles", 10))

    keep = [a for a in state.get("summarized", []) if a["is_relevant"] and a["relevance"] >= threshold]
    keep.sort(key=lambda a: (a["relevance"], a["rank_hint"]), reverse=True)
    selected = keep[:max_articles]

    if not selected and state.get("summarized"):
        # never hand back an empty digest when there was material -- show the best 3
        fallback = sorted(state["summarized"], key=lambda a: a["relevance"], reverse=True)[:3]
        selected = fallback
        run.log(f"Nothing cleared the relevance bar of {threshold}; "
                f"showing the {len(selected)} closest item(s) instead.")
    else:
        run.log(f"Selected {len(selected)} article(s) at or above relevance {threshold}.")

    stats = dict(state.get("stats", {}))
    stats["selected"] = len(selected)
    return {"selected": selected, "stats": stats}


# -- node 6: executive brief ------------------------------------------------ #
def node_brief(state: AgentState, run: "Runner") -> AgentState:
    selected = state.get("selected", [])
    if not selected:
        return {"headline": "No qualifying stories today", "themes": []}

    if run.llm.available:
        run.log("Writing the executive brief...")
        listing = "\n\n".join(
            f"[{i + 1}] {a['title']} ({a['source']}, {a['topic']}, relevance {a['relevance']})\n"
            + "\n".join(f"  - {b}" for b in a["bullets"])
            for i, a in enumerate(selected)
        )
        data = run.llm.json_call(
            BRIEF_SYSTEM,
            f"READER PROFILE\n{state['settings'].get('profile', '')}\n\nTODAY'S STORIES\n{listing}",
            BRIEF_SCHEMA,
            effort="medium",
            max_tokens=4096,
        )
        if data:
            themes = [str(t).strip() for t in (data.get("themes") or []) if str(t).strip()][:5]
            return {"headline": str(data.get("headline") or "")[:160], "themes": themes}

    topics: Dict[str, int] = {}
    for article in selected:
        topics[article["topic"]] = topics.get(article["topic"], 0) + 1
    ordered = sorted(topics.items(), key=lambda kv: kv[1], reverse=True)
    plural = "story" if len(selected) == 1 else "stories"
    return {
        "headline": f"{len(selected)} {plural} across {len(ordered)} "
                    f"{'topic' if len(ordered) == 1 else 'topics'}",
        "themes": [f"{name} — {count} {'story' if count == 1 else 'stories'}"
                   for name, count in ordered[:5]],
    }


# -- node 7: persist -------------------------------------------------------- #
def node_persist(state: AgentState, run: "Runner") -> AgentState:
    selected = state.get("selected", [])
    stats = state.get("stats", {})
    headline = state.get("headline") or (
        f"No new stories in the last {state['settings'].get('lookback_hours', 36)}h "
        f"({stats.get('already_seen', 0)} already digested)"
    )
    digest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "headline": headline,
        "themes": state.get("themes", []),
        "articles": selected,
        "stats": state.get("stats", {}),
        "errors": state.get("errors", []),
        "engine": selected[0]["engine"] if selected else "none",
        "profile": state["settings"].get("profile", ""),
    }
    # Remember everything we actually considered -- the stories we published, the
    # ones we summarized and rejected, and the duplicate headlines they absorbed --
    # so tomorrow's run does not serve the same news again.
    mark_seen(selected + state.get("summarized", []) + state.get("duplicates", []))
    digest["id"] = store_digest(digest)
    run.log(f"Saved digest #{digest['id']}.")
    return {"digest": digest}


_NODE_OUTPUT_KEY = {
    "fetch": "articles", "dedupe": "articles", "prefilter": "candidates",
    "summarize": "summarized", "rank": "selected", "brief": "themes",
}


def _tracked(name: str, fn, run: "Runner"):
    """Wrap a node so the dashboard can show which step the agent is on."""
    def inner(state: AgentState) -> AgentState:
        _set_node(name, status="running", started=time.time())
        began = time.time()
        try:
            out = fn(state, run)
        except Exception:
            _set_node(name, status="error", secs=round(time.time() - began, 2))
            raise
        key = _NODE_OUTPUT_KEY.get(name)
        count = len(out.get(key) or []) if key else (out.get("digest") or {}).get("id")
        _set_node(name, status="done", secs=round(time.time() - began, 2), count=count)
        return out
    return inner


def build_graph(run: "Runner"):
    """Wire the nodes into a LangGraph StateGraph."""
    graph = StateGraph(AgentState)
    graph.add_node("fetch", _tracked("fetch", node_fetch, run))
    graph.add_node("dedupe", _tracked("dedupe", node_dedupe, run))
    graph.add_node("prefilter", _tracked("prefilter", node_prefilter, run))
    graph.add_node("summarize", _tracked("summarize", node_summarize, run))
    graph.add_node("rank", _tracked("rank", node_rank, run))
    graph.add_node("brief", _tracked("brief", node_brief, run))
    graph.add_node("persist", _tracked("persist", node_persist, run))

    graph.set_entry_point("fetch")
    graph.add_edge("fetch", "dedupe")
    graph.add_edge("dedupe", "prefilter")
    # short-circuit when there is nothing worth summarizing
    graph.add_conditional_edges(
        "prefilter",
        lambda s: "summarize" if s.get("candidates") else "persist",
        {"summarize": "summarize", "persist": "persist"},
    )
    graph.add_edge("summarize", "rank")
    graph.add_edge("rank", "brief")
    graph.add_edge("brief", "persist")
    graph.add_edge("persist", END)
    return graph.compile()


# =========================================================================== #
#  RENDERING + DELIVERY
# =========================================================================== #
def digest_markdown(digest: Dict[str, Any]) -> str:
    when = datetime.fromisoformat(digest["created_at"]).astimezone()
    lines = [
        f"# Daily Industry Digest -- {when:%A %d %B %Y, %H:%M}",
        "",
        f"**{digest.get('headline', '')}**",
        "",
    ]
    for theme in digest.get("themes", []):
        lines.append(f"- {theme}")
    lines += ["", "---", ""]
    for index, article in enumerate(digest.get("articles", []), 1):
        lines += [
            f"## {index}. {article['title']}",
            f"*{article['source']} · {human_age(article['published'])} · "
            f"{article['topic']} · relevance {article['relevance']}/100*",
            "",
        ]
        lines += [f"- {b}" for b in article["bullets"]]
        if article.get("why_it_matters"):
            lines += ["", f"**Why it matters:** {article['why_it_matters']}"]
        lines += ["", f"[Read the original]({article['link']})", ""]
    stats = digest.get("stats", {})
    lines += [
        "---",
        f"_{stats.get('fetched', 0)} articles scanned from {stats.get('feeds', 0)} feeds · "
        f"{stats.get('to_summarize', 0)} summarized · {len(digest.get('articles', []))} selected · "
        f"engine: {digest.get('engine', 'n/a')}_",
    ]
    return "\n".join(lines)


def digest_slack_text(digest: Dict[str, Any]) -> str:
    when = datetime.fromisoformat(digest["created_at"]).astimezone()
    parts = [f"*Daily Industry Digest* — {when:%a %d %b, %H:%M}",
             f"_{digest.get('headline', '')}_", ""]
    for theme in digest.get("themes", []):
        parts.append(f"• {theme}")
    parts.append("")
    for index, article in enumerate(digest.get("articles", []), 1):
        parts.append(f"*{index}. <{article['link']}|{article['title']}>*")
        parts.append(f"_{article['source']} · {human_age(article['published'])} · "
                     f"relevance {article['relevance']}_")
        parts += [f"   • {b}" for b in article["bullets"]]
        if article.get("why_it_matters"):
            parts.append(f"   _Why it matters: {article['why_it_matters']}_")
        parts.append("")
    return "\n".join(parts)[:38000]


def digest_html(digest: Dict[str, Any]) -> str:
    when = datetime.fromisoformat(digest["created_at"]).astimezone()
    blocks = [
        "<div style=\"font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;"
        "max-width:680px;margin:0 auto;color:#16181d;line-height:1.55\">",
        f"<h1 style=\"font-size:21px;margin:0 0 4px\">Daily Industry Digest</h1>",
        f"<p style=\"color:#667;margin:0 0 18px;font-size:13px\">{when:%A %d %B %Y, %H:%M}</p>",
        f"<p style=\"font-size:16px;font-weight:600;margin:0 0 8px\">"
        f"{html_escape(digest.get('headline', ''))}</p>",
        "<ul style=\"margin:0 0 24px;padding-left:20px\">",
    ]
    for theme in digest.get("themes", []):
        blocks.append(f"<li>{html_escape(theme)}</li>")
    blocks.append("</ul>")
    for index, article in enumerate(digest.get("articles", []), 1):
        blocks += [
            "<div style=\"border-top:1px solid #e6e8ec;padding:16px 0\">",
            f"<a href=\"{html_escape(article['link'])}\" style=\"font-size:16px;font-weight:600;"
            f"color:#14306b;text-decoration:none\">{index}. {html_escape(article['title'])}</a>",
            f"<p style=\"color:#777;font-size:12px;margin:4px 0 10px\">"
            f"{html_escape(article['source'])} &middot; {human_age(article['published'])} "
            f"&middot; {html_escape(article['topic'])} &middot; relevance "
            f"{article['relevance']}/100</p>",
            "<ul style=\"margin:0;padding-left:20px\">",
        ]
        blocks += [f"<li>{html_escape(b)}</li>" for b in article["bullets"]]
        blocks.append("</ul>")
        if article.get("why_it_matters"):
            blocks.append(
                f"<p style=\"margin:10px 0 0;font-size:13px;color:#444\"><b>Why it matters:</b> "
                f"{html_escape(article['why_it_matters'])}</p>"
            )
        blocks.append("</div>")
    blocks.append("</div>")
    return "".join(blocks)


def send_to_slack(digest: Dict[str, Any]) -> str:
    webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        raise RuntimeError("SLACK_WEBHOOK_URL is not set in the environment.")
    body = json.dumps({"text": digest_slack_text(digest)}).encode("utf-8")
    request = urllib.request.Request(
        webhook, data=body,
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=20, context=_SSL_CTX) as response:
        response.read()
    return "Posted to Slack."


def send_email(digest: Dict[str, Any]) -> str:
    host = os.getenv("SMTP_HOST", "").strip()
    recipients = [r.strip() for r in os.getenv("EMAIL_TO", "").split(",") if r.strip()]
    if not host or not recipients:
        raise RuntimeError("Set SMTP_HOST, SMTP_USER, SMTP_PASS, EMAIL_FROM and EMAIL_TO first.")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER", "")
    password = os.getenv("SMTP_PASS", "")
    sender = os.getenv("EMAIL_FROM") or user

    message = EmailMessage()
    when = datetime.fromisoformat(digest["created_at"]).astimezone()
    message["Subject"] = f"Daily Industry Digest — {when:%d %b} — {digest.get('headline', '')}"[:200]
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content(digest_markdown(digest))
    message.add_alternative(digest_html(digest), subtype="html")

    if port == 465:
        server = smtplib.SMTP_SSL(host, port, timeout=30, context=_SSL_CTX)
    else:
        server = smtplib.SMTP(host, port, timeout=30)
    with server:
        if port != 465:
            server.starttls(context=_SSL_CTX)
        if user:
            server.login(user, password)
        server.send_message(message)
    return f"Emailed {len(recipients)} recipient(s)."


# =========================================================================== #
#  RUN ORCHESTRATION (one run at a time, with a live log the UI polls)
# =========================================================================== #
RUN_LOCK = threading.Lock()
RUN_STATE: Dict[str, Any] = {
    "status": "idle",        # idle | running | done | error
    "logs": [],
    "started_at": None,
    "finished_at": None,
    "digest_id": None,
    "error": None,
    "nodes": {},             # per-node status/timing/count for the pipeline view
}


def _set_node(name: str, **fields: Any) -> None:
    with RUN_LOCK:
        RUN_STATE["nodes"].setdefault(name, {}).update(fields)


def _set_progress(name: str, done: int, total: int) -> None:
    _set_node(name, done=done, total=total)


def _log(message: str) -> None:
    stamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{stamp}] {message}"
    with RUN_LOCK:
        RUN_STATE["logs"].append(line)
        RUN_STATE["logs"] = RUN_STATE["logs"][-400:]
    print(line, flush=True)


def run_pipeline(settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Execute the whole LangGraph pipeline once and return the digest."""
    settings = settings or load_settings()
    runner = Runner(log=_log)
    runner.llm.model = settings.get("model", MODEL)
    app_graph = build_graph(runner)
    _log("Graph compiled: fetch -> dedupe -> prefilter -> summarize -> rank -> brief -> persist")
    final: AgentState = app_graph.invoke({"settings": settings})
    digest = final.get("digest") or {}
    _log(f"Done: {len(digest.get('articles', []))} article(s) in the digest.")
    return digest


def _run_in_background() -> None:
    try:
        digest = run_pipeline()
        with RUN_LOCK:
            RUN_STATE["status"] = "done"
            RUN_STATE["digest_id"] = digest.get("id")
            RUN_STATE["finished_at"] = datetime.now(timezone.utc).isoformat()
    except Exception as exc:
        _log(f"RUN FAILED: {type(exc).__name__}: {exc}")
        with RUN_LOCK:
            RUN_STATE["status"] = "error"
            RUN_STATE["error"] = f"{type(exc).__name__}: {exc}"
            RUN_STATE["finished_at"] = datetime.now(timezone.utc).isoformat()


def start_run() -> bool:
    with RUN_LOCK:
        if RUN_STATE["status"] == "running":
            return False
        RUN_STATE.update({
            "status": "running", "logs": [], "error": None,
            "digest_id": None, "finished_at": None, "nodes": {},
            "started_at": datetime.now(timezone.utc).isoformat(),
        })
    threading.Thread(target=_run_in_background, daemon=True).start()
    return True


def scheduler_loop(run_at: str) -> None:
    """Fire one run a day at local HH:MM, then deliver if a channel is configured."""
    last_day = None
    while True:
        now = datetime.now()
        if now.strftime("%H:%M") == run_at and last_day != now.date():
            last_day = now.date()
            _log(f"Scheduled run for {run_at}.")
            if start_run():
                while RUN_STATE["status"] == "running":
                    time.sleep(2)
                digest = get_digest()
                if digest and digest.get("articles"):
                    for sender, label in ((send_to_slack, "Slack"), (send_email, "email")):
                        try:
                            _log(sender(digest))
                        except Exception as exc:
                            _log(f"   (skipped {label}: {exc})")
        time.sleep(20)


# =========================================================================== #
#  WEB APP
# =========================================================================== #
app = FastAPI(title="Daily Industry News Summarizer Agent")


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return PAGE


@app.get("/api/settings")
def api_get_settings() -> Dict[str, Any]:
    settings = load_settings()
    settings["_llm_ready"] = bool(
        anthropic is not None and (os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_AUTH_TOKEN"))
    )
    settings["_slack_ready"] = bool(os.getenv("SLACK_WEBHOOK_URL"))
    settings["_email_ready"] = bool(os.getenv("SMTP_HOST") and os.getenv("EMAIL_TO"))
    return settings


@app.post("/api/settings")
def api_save_settings(patch: Dict[str, Any]) -> Dict[str, Any]:
    return save_settings(patch)


@app.post("/api/run")
def api_run() -> Dict[str, Any]:
    started = start_run()
    return {"started": started, "status": RUN_STATE["status"]}


@app.get("/api/status")
def api_status() -> Dict[str, Any]:
    with RUN_LOCK:
        return json.loads(json.dumps(RUN_STATE))   # deep copy, safe to serialise


@app.get("/api/digest/latest")
def api_latest() -> Any:
    digest = get_digest()
    if not digest:
        return JSONResponse({"empty": True})
    return digest


@app.get("/api/digest/{digest_id}")
def api_digest(digest_id: int) -> Any:
    digest = get_digest(digest_id)
    if not digest:
        raise HTTPException(status_code=404, detail="No digest with that id")
    return digest


@app.get("/api/digest/{digest_id}/markdown", response_class=PlainTextResponse)
def api_markdown(digest_id: int) -> str:
    digest = get_digest(digest_id)
    if not digest:
        raise HTTPException(status_code=404, detail="No digest with that id")
    return digest_markdown(digest)


@app.get("/api/history")
def api_history() -> List[Dict[str, Any]]:
    return list_digests()


@app.post("/api/deliver/{channel}/{digest_id}")
def api_deliver(channel: str, digest_id: int) -> Dict[str, Any]:
    digest = get_digest(digest_id)
    if not digest:
        raise HTTPException(status_code=404, detail="No digest with that id")
    try:
        if channel == "slack":
            return {"ok": True, "message": send_to_slack(digest)}
        if channel == "email":
            return {"ok": True, "message": send_email(digest)}
    except Exception as exc:
        return {"ok": False, "message": f"{type(exc).__name__}: {exc}"}
    raise HTTPException(status_code=400, detail="Unknown channel")


# --------------------------------------------------------------------------- #
#  The dashboard (single page, no build step, no CDN dependencies)
# --------------------------------------------------------------------------- #
PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Newsroom Agent</title>
<style>
  @property --v { syntax:'<number>'; inherits:false; initial-value:0; }
  :root {
    --bg:#f4f5f9; --panel:#ffffff; --panel-2:#f8f9fc; --ink:#14171f; --muted:#69708a;
    --line:#e5e8f0; --accent:#4f46e5; --accent-2:#7c3aed; --accent-soft:#eef0ff;
    --good:#0e9f6e; --good-soft:#e6f6ef; --warn:#b45309; --warn-soft:#fdf3e5;
    --bad:#d92d20; --bad-soft:#fdecea; --console:#0f1220; --console-ink:#c9d1e3;
    --shadow:0 1px 2px rgba(16,24,40,.05), 0 8px 24px rgba(16,24,40,.06);
    --ease:cubic-bezier(.2,.8,.2,1);
    color-scheme: light;
  }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) {
      --bg:#0b0d12; --panel:#14171f; --panel-2:#191d27; --ink:#e8eaf0; --muted:#98a0b6;
      --line:#262b39; --accent:#8b8cff; --accent-2:#b18cff; --accent-soft:#1d2040;
      --good:#34d399; --good-soft:#0f2a22; --warn:#fbbf24; --warn-soft:#2c2412;
      --bad:#f87171; --bad-soft:#2d1517; --console:#080a10; --console-ink:#b9c2d6;
      --shadow:0 1px 2px rgba(0,0,0,.4), 0 10px 30px rgba(0,0,0,.35);
      color-scheme: dark;
    }
  }
  :root[data-theme="dark"] {
    --bg:#0b0d12; --panel:#14171f; --panel-2:#191d27; --ink:#e8eaf0; --muted:#98a0b6;
    --line:#262b39; --accent:#8b8cff; --accent-2:#b18cff; --accent-soft:#1d2040;
    --good:#34d399; --good-soft:#0f2a22; --warn:#fbbf24; --warn-soft:#2c2412;
    --bad:#f87171; --bad-soft:#2d1517; --console:#080a10; --console-ink:#b9c2d6;
    --shadow:0 1px 2px rgba(0,0,0,.4), 0 10px 30px rgba(0,0,0,.35);
    color-scheme: dark;
  }
  * { box-sizing:border-box; }
  html { scroll-behavior:smooth; }
  body {
    margin:0; background:var(--bg); color:var(--ink); line-height:1.55; font-size:14.5px;
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Inter,Helvetica,Arial,sans-serif;
    animation:pagein .5s var(--ease) both;
  }
  @keyframes pagein { from { opacity:0; transform:translateY(6px); } to { opacity:1; transform:none; } }
  button, input, select, textarea { font:inherit; color:inherit; }
  a { color:var(--accent); }
  kbd { font-family:ui-monospace,Consolas,monospace; font-size:11px; padding:1px 6px;
        border:1px solid var(--line); border-bottom-width:2px; border-radius:5px; background:var(--panel-2); }
  :focus-visible { outline:2px solid var(--accent); outline-offset:2px; }

  /* ---------- top progress bar ---------- */
  #topbar { position:fixed; top:0; left:0; height:3px; width:0; z-index:60; opacity:0;
            background:linear-gradient(90deg, var(--accent), var(--accent-2));
            transition:width .5s var(--ease), opacity .4s; box-shadow:0 0 10px var(--accent); }
  #topbar.on { opacity:1; }

  /* ---------- header ---------- */
  header {
    position:sticky; top:0; z-index:30; background:color-mix(in srgb, var(--panel) 86%, transparent);
    backdrop-filter:blur(12px); border-bottom:1px solid transparent;
    padding:12px 20px; display:flex; align-items:center; gap:10px; flex-wrap:wrap;
    transition:border-color .2s, box-shadow .2s;
  }
  header.scrolled { border-bottom-color:var(--line); box-shadow:0 4px 18px rgba(16,24,40,.06); }
  .brand { display:flex; align-items:center; gap:11px; margin-right:6px; }
  .orb { width:32px; height:32px; border-radius:50%; flex:none;
         background:radial-gradient(circle at 30% 30%, #fff9, transparent 45%),
                    linear-gradient(135deg, var(--accent), var(--accent-2)); }
  body.running .orb { animation:pulse 1.4s ease-out infinite; }
  @keyframes pulse { 0% { box-shadow:0 0 0 0 color-mix(in srgb, var(--accent) 55%, transparent); }
                     100% { box-shadow:0 0 0 14px transparent; } }
  .brand h1 { font-size:16px; margin:0; font-weight:700; letter-spacing:-.01em; line-height:1.2; }
  .brand small { display:flex; align-items:center; gap:6px; font-size:12px; color:var(--muted); font-weight:500; }
  .brand small i { width:7px; height:7px; border-radius:50%; background:var(--muted); display:inline-block; transition:.3s; }
  body.running .brand small i { background:var(--good); animation:blinkdot 1s ease-in-out infinite; }
  @keyframes blinkdot { 50% { opacity:.25; } }
  .spacer { flex:1 1 auto; }
  .pill { font-size:12px; padding:4px 11px; border-radius:999px; border:1px solid var(--line);
          color:var(--muted); white-space:nowrap; display:inline-flex; align-items:center; gap:6px; cursor:default; }
  .pill::before { content:""; width:7px; height:7px; border-radius:50%; background:currentColor; opacity:.8; }
  .pill.on { color:var(--good); background:var(--good-soft); border-color:transparent; }
  .pill.off { color:var(--warn); background:var(--warn-soft); border-color:transparent; }

  .btn { cursor:pointer; border-radius:10px; padding:8px 14px; border:1px solid var(--line);
         background:var(--panel); font-size:13.5px; transition:transform .15s var(--ease), border-color .15s, color .15s, background .15s, box-shadow .15s;
         display:inline-flex; align-items:center; gap:7px; white-space:nowrap; text-decoration:none; color:var(--ink); }
  .btn:hover:not(:disabled) { border-color:var(--accent); color:var(--accent); transform:translateY(-1px); }
  .btn:active:not(:disabled) { transform:translateY(0) scale(.98); }
  .btn:disabled { opacity:.45; cursor:not-allowed; }
  .btn.icon { padding:8px 10px; }
  .btn.sm { padding:5px 10px; font-size:12.5px; border-radius:8px; }
  .btn.primary { background:linear-gradient(135deg, var(--accent), var(--accent-2)); color:#fff;
                 border-color:transparent; font-weight:600; box-shadow:0 4px 16px color-mix(in srgb, var(--accent) 38%, transparent); }
  .btn.primary:hover:not(:disabled) { color:#fff; filter:brightness(1.07); box-shadow:0 6px 22px color-mix(in srgb, var(--accent) 48%, transparent); }
  .spin { width:13px; height:13px; border:2px solid currentColor; border-right-color:transparent;
          border-radius:50%; animation:sp .7s linear infinite; display:inline-block; }
  @keyframes sp { to { transform:rotate(360deg); } }

  .page { max-width:1280px; margin:0 auto; padding:14px 16px 80px; }
  .card { background:var(--panel); border:1px solid var(--line); border-radius:16px;
          padding:20px; box-shadow:var(--shadow); }
  .card-h { display:flex; align-items:center; gap:10px; margin:0 0 14px; flex-wrap:wrap; }
  .card-h h2 { font-size:12px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted); margin:0; font-weight:700; }
  .sub { font-size:12.5px; color:var(--muted); }

  /* ---------- pipeline ---------- */
  .pipeline { display:grid; grid-template-columns:repeat(7, minmax(0,1fr)); position:relative; }
  .node { position:relative; text-align:center; padding:0 4px; }
  .node::before { content:""; position:absolute; top:20px; left:calc(-50% + 24px); right:calc(50% + 24px);
                  height:2px; background:var(--line); border-radius:2px; transition:background .4s; }
  .node:first-child::before { display:none; }
  .node.done::before { background:linear-gradient(90deg, var(--accent), var(--accent-2)); }
  .node.running::before { background-image:linear-gradient(90deg, var(--line), var(--accent), var(--line));
                          background-size:200% 100%; animation:flow 1s linear infinite; }
  @keyframes flow { from { background-position:100% 0; } to { background-position:-100% 0; } }
  .dot { width:42px; height:42px; margin:0 auto 9px; border-radius:50%; display:grid; place-items:center;
         border:2px solid var(--line); background:var(--panel); color:var(--muted); font-weight:700;
         font-size:13px; position:relative; z-index:1; transition:background .3s, border-color .3s, color .3s, box-shadow .3s; }
  .dot.pop { animation:pop .45s var(--ease); }
  @keyframes pop { 0% { transform:scale(.7); } 60% { transform:scale(1.14); } 100% { transform:scale(1); } }
  .node.running .dot { border-color:var(--accent); color:var(--accent); box-shadow:0 0 0 7px var(--accent-soft); }
  .node.done .dot { background:linear-gradient(135deg, var(--accent), var(--accent-2)); color:#fff; border-color:transparent; }
  .node.skipped .dot { border-style:dashed; opacity:.55; }
  .node.error .dot { border-color:var(--bad); color:var(--bad); background:var(--bad-soft); }
  .node b { display:block; font-size:13px; }
  .node .d { font-size:11.5px; color:var(--muted); line-height:1.35; }
  .node .m { font-size:11.5px; margin-top:5px; min-height:18px; color:var(--accent); font-weight:600; font-variant-numeric:tabular-nums; }
  .node.skipped .m { color:var(--muted); font-weight:500; }
  .bar { height:4px; border-radius:4px; background:var(--line); overflow:hidden; margin:6px auto 0; width:70%; }
  .bar i { display:block; height:100%; width:0; background:linear-gradient(90deg, var(--accent), var(--accent-2)); transition:width .4s var(--ease); }
  @media (max-width:820px) {
    .pipeline { grid-template-columns:1fr; gap:12px; }
    .node { display:grid; grid-template-columns:42px 1fr auto; gap:12px; text-align:left; align-items:center; }
    .node::before { display:none; }
    .dot { margin:0; }
    .node .bar { grid-column:2 / 4; margin:0; width:100%; }
  }
  .narr { margin:16px 0 0; padding:10px 14px; border-radius:11px; background:var(--panel-2); border:1px solid var(--line);
          font-size:13.5px; color:var(--muted); display:flex; align-items:center; gap:10px; min-height:42px; }
  .narr .tick { width:8px; height:8px; border-radius:50%; background:var(--muted); flex:none; transition:background .3s; }
  body.running .narr .tick { background:var(--accent); animation:blinkdot 1s ease-in-out infinite; }
  .narr span.t { transition:opacity .25s, transform .25s; }
  .narr span.t.swap { opacity:0; transform:translateY(4px); }
  body.running .narr { color:var(--ink); }

  .funnel { display:grid; grid-template-columns:repeat(4, minmax(0,1fr)); gap:12px; margin-top:14px; }
  .fstep { background:var(--panel-2); border:1px solid var(--line); border-radius:12px; padding:11px 14px; }
  .fstep span { font-size:11.5px; color:var(--muted); text-transform:uppercase; letter-spacing:.06em; font-weight:600; }
  .fstep strong { display:block; font-size:26px; font-weight:750; letter-spacing:-.02em; font-variant-numeric:tabular-nums; line-height:1.25; }
  .fstep .track { height:5px; border-radius:5px; background:var(--line); margin-top:6px; overflow:hidden; }
  .fstep .track i { display:block; height:100%; background:linear-gradient(90deg, var(--accent), var(--accent-2)); border-radius:5px; transition:width .8s var(--ease); }
  @media (max-width:620px) { .funnel { grid-template-columns:repeat(2, minmax(0,1fr)); } }

  /* ---------- layout ---------- */
  .grid { display:grid; grid-template-columns:minmax(0,1fr) 340px; gap:18px; margin-top:18px; align-items:start; }
  @media (max-width:1000px) { .grid { grid-template-columns:1fr; } }
  aside { display:flex; flex-direction:column; gap:18px; position:sticky; top:78px; }
  @media (max-width:1000px) { aside { position:static; } }

  /* ---------- console ---------- */
  .collapse-btn { margin-left:auto; border:0; background:none; cursor:pointer; color:var(--muted); padding:4px 6px; border-radius:6px; }
  .collapse-btn:hover { color:var(--accent); background:var(--accent-soft); }
  .collapse-btn svg { transition:transform .25s var(--ease); display:block; }
  .card.collapsed .collapse-btn svg { transform:rotate(-90deg); }
  .fold { display:grid; grid-template-rows:1fr; transition:grid-template-rows .3s var(--ease); }
  .fold > div { overflow:hidden; min-height:0; }
  .card.collapsed .fold { grid-template-rows:0fr; }
  .card.collapsed .card-h { margin-bottom:0; }
  .console { background:var(--console); color:var(--console-ink); border-radius:11px; padding:11px 13px;
             height:250px; overflow:auto; font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
             font-size:11.5px; line-height:1.65; }
  .console div { white-space:pre-wrap; word-break:break-word; animation:fadein .25s ease; }
  .console .t { color:#6b7591; margin-right:7px; }
  .console .ok { color:#5eead4; } .console .err { color:#fca5a5; } .console .warn { color:#fcd34d; }
  .console .step { color:#c4b5fd; font-weight:600; }
  .console .caret::after { content:"▋"; animation:blink 1s steps(1) infinite; color:#8b8cff; }
  @keyframes blink { 50% { opacity:0; } }
  @keyframes fadein { from { opacity:0; transform:translateY(4px); } to { opacity:1; transform:none; } }

  .hist { list-style:none; margin:0; padding:0; max-height:250px; overflow:auto; }
  .hist li { display:flex; justify-content:space-between; align-items:center; gap:8px; padding:8px 10px;
             border-radius:9px; cursor:pointer; font-size:13px; transition:background .15s; }
  .hist li:hover { background:var(--panel-2); }
  .hist li.active { background:var(--accent-soft); color:var(--accent); font-weight:600; }
  .hist .n { font-size:11.5px; color:var(--muted); font-weight:500; }

  /* ---------- digest ---------- */
  .headline { font-size:24px; font-weight:760; letter-spacing:-.02em; margin:6px 0 14px; line-height:1.25; }
  .themes { list-style:none; margin:0; padding:0; display:grid; gap:8px; counter-reset:th; }
  .themes li { counter-increment:th; padding:10px 14px 10px 46px; background:var(--panel-2); border-radius:11px;
               font-size:14px; position:relative; animation:fadein .4s var(--ease) both; }
  .themes li::before { content:counter(th); position:absolute; left:12px; top:50%; transform:translateY(-50%);
                       width:22px; height:22px; border-radius:50%; display:grid; place-items:center; font-size:11.5px; font-weight:700;
                       background:var(--accent-soft); color:var(--accent); }
  .meta { font-size:12px; color:var(--muted); display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  .tag { font-size:11.5px; padding:2px 9px; border-radius:999px; border:1px solid var(--line); color:var(--muted); }
  .tag.accent { color:var(--accent); border-color:color-mix(in srgb, var(--accent) 45%, var(--line)); }
  .toolbar { display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin:20px 0 10px; padding-top:18px; border-top:1px solid var(--line); }
  .search { flex:1 1 200px; position:relative; }
  .search input { width:100%; padding:9px 12px 9px 34px; border-radius:10px; border:1px solid var(--line); background:var(--panel-2); font-size:13.5px; transition:border-color .15s, box-shadow .15s; }
  .search input:focus, select:focus, textarea:focus, .field input:focus { outline:none; border-color:var(--accent); box-shadow:0 0 0 3px var(--accent-soft); }
  .search svg { position:absolute; left:11px; top:50%; transform:translateY(-50%); color:var(--muted); }
  select { padding:9px 10px; border-radius:10px; border:1px solid var(--line); background:var(--panel-2); font-size:13px; cursor:pointer; }
  .chips { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:4px; }
  .chip { cursor:pointer; font-size:12.5px; padding:4px 12px; border-radius:999px; border:1px solid var(--line);
          background:var(--panel); transition:all .18s var(--ease); user-select:none; }
  .chip:hover { border-color:var(--accent); transform:translateY(-1px); }
  .chip.on { background:var(--accent); border-color:var(--accent); color:#fff; }
  .chip .c { opacity:.7; margin-left:5px; font-size:11px; }
  .seg { display:inline-flex; border:1px solid var(--line); border-radius:10px; overflow:hidden; }
  .seg button { border:0; background:var(--panel); padding:8px 12px; font-size:12.5px; cursor:pointer; color:var(--muted); transition:.15s; }
  .seg button.on { background:var(--accent-soft); color:var(--accent); font-weight:600; }

  /* share menu */
  .menu { position:relative; }
  .menu-pop { position:absolute; right:0; top:calc(100% + 6px); min-width:210px; background:var(--panel); border:1px solid var(--line);
              border-radius:12px; box-shadow:var(--shadow); padding:6px; z-index:20; animation:menuin .18s var(--ease); }
  @keyframes menuin { from { opacity:0; transform:translateY(-6px) scale(.97); } to { opacity:1; transform:none; } }
  .menu-pop button { display:flex; width:100%; align-items:center; gap:9px; text-align:left; border:0; background:none; cursor:pointer;
                     padding:8px 10px; border-radius:8px; font-size:13px; }
  .menu-pop button:hover:not(:disabled) { background:var(--accent-soft); color:var(--accent); }
  .menu-pop button:disabled { opacity:.5; cursor:not-allowed; }
  .menu-pop small { margin-left:auto; color:var(--muted); font-size:11px; }

  /* ---------- story cards ---------- */
  .story { border:1px solid var(--line); border-radius:14px; padding:16px 18px; margin-top:12px; background:var(--panel);
           display:grid; grid-template-columns:52px minmax(0,1fr); gap:16px; position:relative;
           transition:border-color .2s, box-shadow .25s, transform .25s var(--ease), opacity .25s;
           animation:cardin .5s var(--ease) both; }
  @keyframes cardin { from { opacity:0; transform:translateY(14px); } to { opacity:1; transform:none; } }
  .story:hover { border-color:color-mix(in srgb, var(--accent) 50%, var(--line)); transform:translateY(-2px); box-shadow:var(--shadow); }
  .story.focus { border-color:var(--accent); box-shadow:0 0 0 3px var(--accent-soft); }
  .story.read { opacity:.55; }
  .story.read:hover { opacity:.9; }
  .ring { --c:var(--accent); --t:0; width:50px; height:50px; border-radius:50%; display:grid; place-items:center;
          background:conic-gradient(var(--c) calc(var(--v) * 1%), var(--line) 0);
          animation:fillring 1s .2s var(--ease) forwards; }
  @keyframes fillring { to { --v:var(--t); } }
  .ring span { width:40px; height:40px; border-radius:50%; background:var(--panel); display:grid; place-items:center;
               font-size:13.5px; font-weight:760; font-variant-numeric:tabular-nums; }
  .story h3 { margin:0 0 5px; font-size:16px; font-weight:660; line-height:1.4; }
  .story h3 a { color:var(--ink); text-decoration:none; background:linear-gradient(var(--accent), var(--accent)) 0 100% / 0 1.5px no-repeat; transition:background-size .3s var(--ease), color .15s; }
  .story h3 a:hover { color:var(--accent); background-size:100% 1.5px; }
  .star { color:var(--warn); margin-left:6px; font-size:13px; animation:pop .4s var(--ease); }
  .av { width:20px; height:20px; border-radius:6px; display:inline-grid; place-items:center; font-size:11px; font-weight:700; color:#fff; flex:none; }
  .src { display:inline-flex; align-items:center; gap:6px; color:var(--ink); font-weight:600; }
  .story ul { margin:11px 0 0; padding-left:18px; }
  .story li { margin-bottom:4px; }
  .story li::marker { color:var(--accent); }
  .why { margin:11px 0 0; font-size:13.5px; color:var(--muted); background:var(--panel-2); border-radius:9px; padding:9px 12px; }
  .why b { color:var(--ink); }
  .acts { position:absolute; top:10px; right:12px; display:flex; gap:4px; padding:4px; border-radius:11px; background:var(--panel);
          border:1px solid var(--line); box-shadow:var(--shadow); opacity:0; transform:translateY(-4px); pointer-events:none;
          transition:opacity .2s, transform .2s var(--ease); }
  .story:hover .acts, .story.focus .acts, .story:focus-within .acts { opacity:1; transform:none; pointer-events:auto; }
  @media (hover:none) { .acts { position:static; opacity:1; transform:none; pointer-events:auto; margin-top:12px; flex-wrap:wrap;
                                border:0; box-shadow:none; padding:0; background:none; } }
  .acts .btn.on { color:var(--warn); border-color:var(--warn); background:var(--warn-soft); }
  body.compact .story ul, body.compact .story .why { display:none; }
  body.compact .story { padding:12px 16px; }

  .empty { text-align:center; color:var(--muted); padding:36px 16px; }
  .empty h3 { color:var(--ink); margin:0 0 6px; font-size:19px; }
  .steps3 { display:grid; grid-template-columns:repeat(3, minmax(0,1fr)); gap:12px; margin:22px 0 20px; text-align:left; }
  @media (max-width:640px) { .steps3 { grid-template-columns:1fr; } }
  .steps3 div { background:var(--panel-2); border:1px solid var(--line); border-radius:12px; padding:14px; animation:cardin .5s var(--ease) both; }
  .steps3 div:nth-child(2) { animation-delay:.08s; } .steps3 div:nth-child(3) { animation-delay:.16s; }
  .steps3 b { display:block; color:var(--ink); margin-bottom:3px; }
  .steps3 em { font-style:normal; display:inline-grid; place-items:center; width:24px; height:24px; border-radius:50%;
               background:var(--accent-soft); color:var(--accent); font-weight:700; font-size:12px; margin-bottom:8px; }
  .steps3 span { font-size:13px; }
  .skeleton { height:92px; border-radius:14px; margin-top:12px;
              background:linear-gradient(90deg, var(--panel-2), var(--line), var(--panel-2)); background-size:200% 100%;
              animation:shimmer 1.3s linear infinite; }
  @keyframes shimmer { from { background-position:100% 0; } to { background-position:-100% 0; } }

  /* ---------- drawer ---------- */
  .scrim { position:fixed; inset:0; background:rgba(8,10,16,.5); z-index:40; opacity:0; pointer-events:none; transition:opacity .25s; backdrop-filter:blur(2px); }
  .scrim.show { opacity:1; pointer-events:auto; }
  .drawer { position:fixed; top:0; right:0; bottom:0; width:min(470px, 100%); background:var(--panel); z-index:41;
            border-left:1px solid var(--line); transform:translateX(104%); transition:transform .32s var(--ease);
            display:flex; flex-direction:column; box-shadow:-20px 0 50px rgba(0,0,0,.18); }
  .drawer.show { transform:none; }
  .dh { padding:16px 20px; border-bottom:1px solid var(--line); display:flex; align-items:center; gap:10px; }
  .dh h2 { margin:0; font-size:16px; flex:1; }
  .tabs { display:flex; gap:4px; padding:10px 20px 0; border-bottom:1px solid var(--line); position:relative; }
  .tabs button { border:0; background:none; padding:9px 13px; cursor:pointer; font-size:13.5px; color:var(--muted);
                 border-bottom:2px solid transparent; margin-bottom:-1px; transition:color .15s, border-color .2s; }
  .tabs button:hover { color:var(--ink); }
  .tabs button.on { color:var(--accent); border-bottom-color:var(--accent); font-weight:600; }
  .db { padding:20px; overflow:auto; flex:1; }
  .db [data-pane] { animation:fadein .3s var(--ease); }
  .df { padding:14px 20px; border-top:1px solid var(--line); display:flex; gap:8px; align-items:center; }
  .df .dirty { font-size:12px; color:var(--warn); flex:1; }
  .field { margin-bottom:20px; }
  .field > label { display:block; font-size:12.5px; font-weight:600; margin-bottom:6px; }
  .field .hint { font-size:12px; color:var(--muted); margin-top:5px; }
  .field textarea, .field input[type=text], .field input[type=url] {
    width:100%; padding:10px 12px; border-radius:10px; border:1px solid var(--line); background:var(--panel-2); font-size:13.5px; resize:vertical; }
  .tagbox { display:flex; flex-wrap:wrap; gap:6px; padding:8px; border:1px solid var(--line); border-radius:10px; background:var(--panel-2); cursor:text; transition:.15s; }
  .tagbox:focus-within { border-color:var(--accent); box-shadow:0 0 0 3px var(--accent-soft); }
  .tagbox .t { font-size:12.5px; padding:3px 6px 3px 10px; border-radius:999px; background:var(--accent-soft); color:var(--accent);
               display:inline-flex; align-items:center; gap:4px; animation:pop .3s var(--ease); }
  .tagbox.ex .t { background:var(--bad-soft); color:var(--bad); }
  .tagbox .t button { border:0; background:none; cursor:pointer; color:inherit; padding:0 3px; font-size:14px; line-height:1; }
  .tagbox input { border:0; outline:0; background:none; flex:1 1 120px; min-width:80px; padding:3px 4px; font-size:13px; }
  .range { display:flex; align-items:center; gap:12px; }
  .range input { flex:1; accent-color:var(--accent); }
  .range output { min-width:52px; text-align:right; font-weight:700; font-variant-numeric:tabular-nums; }
  .feed { display:flex; gap:6px; align-items:center; margin-bottom:6px; }
  .feed input { flex:1; padding:8px 10px; border-radius:9px; border:1px solid var(--line); background:var(--panel-2); font-size:12.5px; }
  .switch { display:flex; align-items:center; gap:10px; cursor:pointer; font-size:13.5px; }
  .switch input { width:38px; height:22px; appearance:none; background:var(--line); border-radius:22px; position:relative; cursor:pointer; transition:.25s; flex:none; }
  .switch input::after { content:""; position:absolute; width:18px; height:18px; border-radius:50%; background:#fff; top:2px; left:2px; transition:.25s var(--ease); box-shadow:0 1px 3px rgba(0,0,0,.3); }
  .switch input:checked { background:var(--accent); }
  .switch input:checked::after { left:18px; }
  .presets { display:flex; gap:6px; flex-wrap:wrap; margin-top:8px; }

  /* ---------- modal + toast ---------- */
  .modal { position:fixed; inset:0; display:grid; place-items:center; z-index:70; padding:16px; }
  .modal[hidden] { display:none; }
  .modal .box { background:var(--panel); border:1px solid var(--line); border-radius:16px; padding:22px; width:min(420px,100%); box-shadow:var(--shadow); position:relative; z-index:1; animation:menuin .25s var(--ease); }
  .modal .bg { position:absolute; inset:0; background:rgba(8,10,16,.5); backdrop-filter:blur(2px); }
  .keys { display:grid; grid-template-columns:auto 1fr; gap:9px 16px; font-size:13.5px; margin:14px 0 0; }
  #toasts { position:fixed; bottom:20px; left:50%; transform:translateX(-50%); z-index:80; display:flex; flex-direction:column; gap:8px; align-items:center; pointer-events:none; }
  .toast { background:var(--ink); color:var(--bg); padding:10px 16px; border-radius:11px; font-size:13.5px; box-shadow:var(--shadow);
           animation:toastin .3s var(--ease); max-width:90vw; transition:opacity .3s, transform .3s; }
  .toast.out { opacity:0; transform:translateY(8px); }
  .toast.good { background:var(--good); color:#fff; } .toast.bad { background:var(--bad); color:#fff; }
  @keyframes toastin { from { opacity:0; transform:translateY(12px) scale(.96); } to { opacity:1; transform:none; } }

  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after { animation:none !important; transition:none !important; scroll-behavior:auto !important; }
    .ring { --v:var(--t); }
  }
</style>
</head>
<body>
<div id="topbar"></div>
<header id="hdr">
  <div class="brand">
    <div class="orb"></div>
    <div><h1>Newsroom Agent</h1><small><i></i><span id="agent-state">Agent idle</span></small></div>
  </div>
  <span class="pill" id="pill-llm">checking…</span>
  <span class="spacer"></span>
  <button class="btn icon" onclick="toggleTheme()" title="Toggle theme" aria-label="Toggle theme">
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>
  </button>
  <button class="btn icon" onclick="showKeys(true)" title="Keyboard shortcuts" aria-label="Keyboard shortcuts">?</button>
  <button class="btn" onclick="openDrawer()">
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3M1 14h6M9 8h6M17 16h6"/></svg>
    Configure
  </button>
  <button class="btn primary" id="btn-run" onclick="runNow()"></button>
</header>

<div class="page">
  <section class="card">
    <div class="card-h">
      <h2>Agent pipeline</h2>
      <span class="sub">LangGraph · 7 steps</span>
      <span class="spacer"></span>
      <span class="sub" id="elapsed"></span>
    </div>
    <div class="pipeline" id="pipeline"></div>
    <div class="narr"><span class="tick"></span><span class="t" id="narr">Ready. Press Run agent or hit R.</span></div>
    <div class="funnel" id="funnel"></div>
  </section>

  <div class="grid">
    <main class="card" id="digest-card"><div id="digest"></div></main>
    <aside>
      <section class="card" id="log-card">
        <div class="card-h">
          <h2>Agent activity</h2>
          <button class="btn sm" onclick="copyLog()" style="margin-left:8px">Copy</button>
          <button class="collapse-btn" onclick="toggleLog()" aria-label="Collapse activity log" title="Collapse">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4"><path d="m6 9 6 6 6-6"/></svg>
          </button>
        </div>
        <div class="fold"><div><div class="console" id="console"><div class="caret"><span class="t">--:--:--</span>Waiting for instructions.</div></div></div></div>
      </section>
      <section class="card">
        <div class="card-h"><h2>Past digests</h2><span class="spacer"></span><span class="sub" id="hist-count"></span></div>
        <ul class="hist" id="history"><li><span class="sub">none yet</span></li></ul>
      </section>
    </aside>
  </div>
</div>

<div class="scrim" id="scrim" onclick="closeDrawer()"></div>
<div class="drawer" id="drawer" role="dialog" aria-label="Agent configuration">
  <div class="dh"><h2>Configure the agent</h2><button class="btn icon" onclick="closeDrawer()" aria-label="Close">✕</button></div>
  <div class="tabs">
    <button class="on" data-tab="audience">Audience</button>
    <button data-tab="filters">Filters</button>
    <button data-tab="sources">Sources</button>
  </div>
  <div class="db">
    <div data-pane="audience">
      <div class="field">
        <label for="profile">Reader profile</label>
        <textarea id="profile" rows="7"></textarea>
        <div class="hint">With an AI model, every article is judged against this. It is the biggest lever on relevance.</div>
        <div class="presets">
          <button class="btn sm" data-preset="exec">Enterprise exec</button>
          <button class="btn sm" data-preset="founder">AI startup founder</button>
          <button class="btn sm" data-preset="security">Security team</button>
          <button class="btn sm" data-preset="investor">VC investor</button>
        </div>
      </div>
      <div class="field">
        <label>Keywords <span style="color:var(--muted);font-weight:400">· press Enter to add</span></label>
        <div class="tagbox" id="kw-box"><input id="kw-input" placeholder="Add keyword…"></div>
      </div>
      <div class="field">
        <label>Exclude articles mentioning</label>
        <div class="tagbox ex" id="ex-box"><input id="ex-input" placeholder="Add exclusion…"></div>
      </div>
    </div>
    <div data-pane="filters" hidden>
      <div class="field">
        <label for="min_relevance">Minimum relevance score</label>
        <div class="range"><input type="range" id="min_relevance" min="0" max="100" step="5"><output id="o-min"></output></div>
        <div class="hint">Stories scored below this are dropped.</div>
      </div>
      <div class="field">
        <label for="max_articles">Maximum stories in a digest</label>
        <div class="range"><input type="range" id="max_articles" min="1" max="30"><output id="o-max"></output></div>
      </div>
      <div class="field">
        <label for="lookback_hours">Lookback window</label>
        <div class="range"><input type="range" id="lookback_hours" min="6" max="168" step="6"><output id="o-look"></output></div>
        <div class="hint">Only articles published within this window are considered.</div>
      </div>
    </div>
    <div data-pane="sources" hidden>
      <div class="field"><label class="switch"><input type="checkbox" id="use_google_news"> Add a Google News search built from my keywords</label></div>
      <div class="field">
        <label>RSS feeds <span style="color:var(--muted);font-weight:400" id="feed-count"></span></label>
        <div id="feeds"></div>
        <div style="display:flex;gap:6px;margin-top:8px">
          <input type="url" id="feed-new" placeholder="https://example.com/feed.xml"
                 style="flex:1;padding:8px 10px;border-radius:9px;border:1px solid var(--line);background:var(--panel-2);font-size:12.5px">
          <button class="btn sm" onclick="addFeed()">Add</button>
        </div>
      </div>
    </div>
  </div>
  <div class="df">
    <span class="dirty" id="dirty"></span>
    <button class="btn" onclick="loadSettings(true)">Revert</button>
    <button class="btn primary" onclick="saveSettings()">Save <kbd style="background:transparent;color:#fff;border-color:#fff6">Ctrl S</kbd></button>
  </div>
</div>

<div class="modal" id="keys" hidden>
  <div class="bg" onclick="showKeys(false)"></div>
  <div class="box">
    <h3 style="margin:0">Keyboard shortcuts</h3>
    <div class="keys">
      <kbd>R</kbd><span>Run the agent</span>
      <kbd>/</kbd><span>Search stories</span>
      <kbd>J</kbd><span>Next story</span>
      <kbd>K</kbd><span>Previous story</span>
      <kbd>O</kbd><span>Open the focused story</span>
      <kbd>S</kbd><span>Save / unsave the focused story</span>
      <kbd>X</kbd><span>Mark the focused story read</span>
      <kbd>C</kbd><span>Toggle compact view</span>
      <kbd>,</kbd><span>Open settings</span>
      <kbd>Esc</kbd><span>Close panels</span>
    </div>
  </div>
</div>
<div id="toasts"></div>

<script>
const NODES = [
  {id:'fetch',     label:'Fetch',     desc:'Pull RSS feeds',      unit:'articles',   say:'Fetching your feeds…'},
  {id:'dedupe',    label:'Dedupe',    desc:'Drop repeats',        unit:'unique',     say:'Removing stories already seen and near-duplicates…'},
  {id:'prefilter', label:'Prefilter', desc:'Keyword scoring',     unit:'candidates', say:'Scoring keywords and picking the best candidates…'},
  {id:'summarize', label:'Summarize', desc:'Bullets per story',   unit:'summarized', say:'Reading and summarizing each candidate…'},
  {id:'rank',      label:'Rank',      desc:'Relevance cut-off',   unit:'selected',   say:'Ranking by relevance…'},
  {id:'brief',     label:'Brief',     desc:'Key themes',          unit:'themes',     say:'Writing the executive brief…'},
  {id:'persist',   label:'Persist',   desc:'Save the digest',     unit:'',           say:'Saving the digest…'},
];
const PRESETS = {
  exec: "Executive and research team at a mid-size enterprise software company. We care about AI/ML platform shifts, model releases and pricing, cloud infrastructure, enterprise SaaS competition, funding and M&A in our space, data privacy and AI regulation, and security incidents that affect enterprise buyers.",
  founder: "Founder of an early-stage AI startup building agentic developer tools. We care about new model releases and API pricing, open-source models, agent frameworks, developer tooling launches, competitor funding rounds, and GPU/compute availability.",
  security: "Security operations team at a large enterprise. We care about breaches, ransomware campaigns, actively exploited vulnerabilities, cloud misconfigurations, AI-enabled attacks, and new security regulation or disclosure rules.",
  investor: "Venture investor focused on AI infrastructure and applications. We care about funding rounds and valuations, M&A, IPO filings, revenue milestones, new market entrants, and regulatory shifts that change market size.",
};

let current = null, polling = null, runStart = null, lastLogCount = 0, userRun = false;
let settings = {}, kw = [], ex = [], feeds = [], dirty = false, pipeBuilt = false, funnelVals = [0,0,0,0];
let view = { q:'', topic:'all', sort:'relevance', saved:false, focus:-1 };
const store = {
  get(k, d) { try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch (e) { return d; } },
  set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} },
};
let saved = new Set(store.get('saved', []));
let readSet = new Set(store.get('read', []));

const $ = id => document.getElementById(id);
const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const api = async (url, opts) => { const r = await fetch(url, opts); return r.json(); };

function toast(msg, kind = '') {
  const el = document.createElement('div');
  el.className = 'toast ' + kind; el.textContent = msg;
  $('toasts').appendChild(el);
  setTimeout(() => { el.classList.add('out'); setTimeout(() => el.remove(), 320); }, 3300);
}
window.addEventListener('scroll', () => $('hdr').classList.toggle('scrolled', scrollY > 6), {passive:true});

/* ---------------- theme ---------------- */
function applyTheme(t) { if (t) document.documentElement.dataset.theme = t; else delete document.documentElement.dataset.theme; }
function toggleTheme() {
  const dark = document.documentElement.dataset.theme
    ? document.documentElement.dataset.theme === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches;
  const next = dark ? 'light' : 'dark'; applyTheme(next); store.set('theme', next);
}
applyTheme(store.get('theme', null));

/* ---------------- pipeline ---------------- */
function buildPipeline() {
  $('pipeline').innerHTML = NODES.map((n, i) => `
    <div class="node" id="n-${n.id}" data-st="">
      <div class="dot"><span class="ic">${i + 1}</span></div>
      <div><b>${n.label}</b><div class="d">${n.desc}</div></div>
      <div class="m"></div>
      ${n.id === 'summarize' ? '<div class="bar"><i></i></div>' : ''}
    </div>`).join('');
  pipeBuilt = true;
}
function updatePipeline(nodes = {}, status = 'idle') {
  if (!pipeBuilt) buildPipeline();
  const any = Object.keys(nodes).length > 0;
  NODES.forEach((n, i) => {
    const s = nodes[n.id] || {};
    const st = s.status || ((status === 'done' || status === 'error') && any ? 'skipped' : '');
    const el = $('n-' + n.id);
    if (el.dataset.st !== st) {
      el.dataset.st = st; el.className = 'node ' + st;
      el.querySelector('.ic').innerHTML = st === 'done' ? '✓' : st === 'running' ? '<span class="spin"></span>'
        : st === 'error' ? '!' : st === 'skipped' ? '–' : (i + 1);
      if (st === 'done' || st === 'running') {
        const dot = el.querySelector('.dot'); dot.classList.remove('pop'); void dot.offsetWidth; dot.classList.add('pop');
      }
    }
    let metric = '';
    if (st === 'done') metric = n.id === 'persist' ? (s.count ? `digest #${s.count}` : 'saved') : `${s.count ?? 0} ${n.unit} · ${s.secs ?? 0}s`;
    else if (st === 'running') metric = s.total ? `${s.done}/${s.total}` : 'working…';
    else if (st === 'skipped') metric = 'skipped';
    else if (st === 'error') metric = 'failed';
    el.querySelector('.m').textContent = metric;
    const bar = el.querySelector('.bar i');
    if (bar) bar.style.width = (st === 'done' ? 100 : s.total ? Math.round(100 * s.done / s.total) : 0) + '%';
  });
}
let fadeTimer = null;
function setTop(nodes, status) {
  const bar = $('topbar');
  const done = NODES.filter(n => (nodes[n.id] || {}).status === 'done').length;
  clearTimeout(fadeTimer);
  if (status === 'running') { bar.classList.add('on'); bar.style.width = Math.max(5, (done + .5) / NODES.length * 100) + '%'; }
  else if (status === 'done' && bar.classList.contains('on')) {
    bar.style.width = '100%';
    fadeTimer = setTimeout(() => { bar.classList.remove('on'); setTimeout(() => bar.style.width = '0', 450); }, 600);
  } else { bar.classList.remove('on'); bar.style.width = '0'; }
}
function narrate(text) {
  const el = $('narr'); if (el.textContent === text) return;
  el.classList.add('swap');
  setTimeout(() => { el.textContent = text; el.classList.remove('swap'); }, 160);
}

function renderFunnel(stats = {}) {
  const vals = [stats.fetched || 0, stats.unique || 0, stats.to_summarize || 0, stats.selected || 0];
  const labels = ['Scanned', 'Unique', 'Analyzed', 'Selected'];
  const top = Math.max(1, vals[0]);
  if (!$('funnel').children.length) {
    $('funnel').innerHTML = labels.map(l => `<div class="fstep"><span>${l}</span><strong>0</strong><div class="track"><i style="width:2%"></i></div></div>`).join('');
  }
  [...$('funnel').children].forEach((box, i) => {
    countUp(box.querySelector('strong'), funnelVals[i], vals[i]);
    box.querySelector('i').style.width = Math.max(2, 100 * vals[i] / top) + '%';
  });
  funnelVals = vals;
}
function countUp(el, from, to) {
  if (matchMedia('(prefers-reduced-motion: reduce)').matches) { el.textContent = to; return; }
  const t0 = performance.now(), dur = 700;
  const step = now => {
    const p = Math.min(1, (now - t0) / dur);
    el.textContent = Math.round(from + (to - from) * (1 - Math.pow(1 - p, 3)));
    if (p < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}

/* ---------------- console ---------------- */
function toggleLog() { $('log-card').classList.toggle('collapsed'); }
function renderLog(lines, running) {
  const box = $('console');
  if (!lines.length) return;
  if (lines.length < lastLogCount) { box.innerHTML = ''; lastLogCount = 0; }
  box.querySelectorAll('.caret').forEach(e => e.classList.remove('caret'));
  const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
  for (const line of lines.slice(lastLogCount)) {
    const m = line.match(/^\[(\d\d:\d\d:\d\d)\] (.*)$/) || [null, '', line];
    const msg = m[2]; let cls = '';
    if (/^\s+x |FAILED|failed/.test(msg)) cls = 'err';
    else if (/^\s+!|No Claude|Nothing cleared/.test(msg)) cls = 'warn';
    else if (/^\s+\+|^Done|^Saved/.test(msg)) cls = 'ok';
    else if (!/^\s/.test(msg)) cls = 'step';
    const div = document.createElement('div');
    div.innerHTML = `<span class="t">${esc(m[1])}</span><span class="${cls}">${esc(msg)}</span>`;
    box.appendChild(div);
  }
  lastLogCount = lines.length;
  if (running && box.lastElementChild) box.lastElementChild.classList.add('caret');
  if (atBottom || running) box.scrollTop = box.scrollHeight;
}
function copyLog() {
  navigator.clipboard.writeText($('console').innerText).then(() => toast('Log copied.'), () => toast('Copy failed', 'bad'));
}

/* ---------------- run ---------------- */
const RUN_LABEL = '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M7 4v16l13-8z"/></svg>Run agent';
async function runNow() {
  if (document.body.classList.contains('running')) return;
  userRun = true; setRunning(true);
  $('console').innerHTML = ''; lastLogCount = 0;
  $('log-card').classList.remove('collapsed');
  updatePipeline({}, 'running'); narrate('Starting the agent…');
  $('digest').innerHTML = `<div class="headline" style="color:var(--muted)">The agent is working…</div>${'<div class="skeleton"></div>'.repeat(4)}`;
  const r = await api('/api/run', {method:'POST'});
  if (!r.started) toast('A run is already in progress.');
  startPolling();
}
function setRunning(on) {
  document.body.classList.toggle('running', on);
  const b = $('btn-run'); b.disabled = on;
  b.innerHTML = on ? '<span class="spin"></span>Working…' : RUN_LABEL;
}
function startPolling() { if (polling) clearInterval(polling); polling = setInterval(poll, 650); poll(); }

async function poll() {
  const s = await api('/api/status');
  const nodes = s.nodes || {};
  renderLog(s.logs || [], s.status === 'running');
  updatePipeline(nodes, s.status);
  setTop(nodes, s.status);
  if (s.status === 'running') {
    setRunning(true);
    const cur = NODES.find(n => (nodes[n.id] || {}).status === 'running');
    $('agent-state').textContent = cur ? `Working · ${cur.label}` : 'Starting…';
    narrate(cur ? cur.say : 'Starting the agent…');
    runStart = s.started_at ? new Date(s.started_at) : runStart;
    if (runStart) $('elapsed').textContent = `${((Date.now() - runStart) / 1000).toFixed(0)}s elapsed`;
    if (!polling) startPolling();
    return;
  }
  if (polling) { clearInterval(polling); polling = null; }
  const wasRunning = document.body.classList.contains('running');
  setRunning(false);
  $('agent-state').textContent = s.status === 'error' ? 'Last run failed' : 'Agent idle';
  if (s.started_at && s.finished_at) $('elapsed').textContent = `finished in ${((new Date(s.finished_at) - new Date(s.started_at)) / 1000).toFixed(1)}s`;
  if (!wasRunning) {
    if (s.status === 'done' && s.started_at && s.finished_at)
      narrate(`Last run finished in ${((new Date(s.finished_at) - new Date(s.started_at)) / 1000).toFixed(1)}s.`);
    return;
  }
  if (s.status === 'error') { narrate('The run failed — see the activity log.'); toast('Run failed: ' + s.error, 'bad'); showDigest(null); return; }
  if (s.status === 'done') {
    await showDigest(s.digest_id); loadHistory();
    const n = (current?.articles || []).length;
    narrate(`Done. ${n} ${n === 1 ? 'story' : 'stories'} made the digest.`);
    toast(`Digest ready · ${n} ${n === 1 ? 'story' : 'stories'}`, 'good');
    if (userRun) { userRun = false; $('digest-card').scrollIntoView({behavior:'smooth', block:'start'}); }
  }
}

/* ---------------- digest ---------------- */
async function showDigest(id) {
  const d = await api(id ? '/api/digest/' + id : '/api/digest/latest');
  if (d.empty) { current = null; renderEmpty(); renderFunnel({}); return; }
  current = d; view.focus = -1; view.topic = 'all'; view.q = '';
  renderFunnel(d.stats || {});
  renderDigest();
  document.querySelectorAll('.hist li').forEach(li => li.classList.toggle('active', +li.dataset.id === d.id));
}
function renderEmpty() {
  $('digest').innerHTML = `<div class="empty">
    <div class="orb" style="width:56px;height:56px;margin:0 auto 14px"></div>
    <h3>Your digest will appear here</h3>
    <p>The agent reads your sources, keeps what matters to you, and summarizes it.</p>
    <div class="steps3">
      <div><em>1</em><b>Collect</b><span>Pulls articles from your RSS feeds and a news search.</span></div>
      <div><em>2</em><b>Judge</b><span>Removes repeats and scores each story against your interests.</span></div>
      <div><em>3</em><b>Brief</b><span>Writes short bullets and the day's key themes.</span></div>
    </div>
    <button class="btn primary" onclick="runNow()">Run agent <kbd style="background:transparent;color:#fff;border-color:#fff6">R</kbd></button>
    <button class="btn" onclick="openDrawer()">Configure first</button></div>`;
}
const relColor = v => v >= 75 ? 'var(--good)' : v >= 55 ? 'var(--accent)' : 'var(--warn)';
const storyKey = a => a.id || a.link;
function avatar(name) {
  let h = 0; for (const c of name) h = (h * 31 + c.charCodeAt(0)) % 360;
  return `<span class="av" style="background:hsl(${h} 52% 46%)">${esc((name.trim()[0] || '?').toUpperCase())}</span>`;
}

function visibleStories() {
  let list = (current?.articles || []).map((a, i) => ({...a, _i:i}));
  if (view.topic !== 'all') list = list.filter(a => a.topic === view.topic);
  if (view.saved) list = list.filter(a => saved.has(storyKey(a)));
  if (view.q) {
    const q = view.q.toLowerCase();
    list = list.filter(a => [a.title, a.source, a.topic, a.why_it_matters, ...(a.bullets || [])].join(' ').toLowerCase().includes(q));
  }
  if (view.sort === 'recent') list.sort((a, b) => b.published.localeCompare(a.published));
  else if (view.sort === 'source') list.sort((a, b) => a.source.localeCompare(b.source));
  else list.sort((a, b) => b.relevance - a.relevance);
  return list;
}

function renderDigest() {
  const d = current, when = new Date(d.created_at), topics = {};
  (d.articles || []).forEach(a => topics[a.topic] = (topics[a.topic] || 0) + 1);
  const slackOk = !!settings._slack_ready, mailOk = !!settings._email_ready;
  $('digest').innerHTML = `
    <div class="meta">
      <span class="tag accent">digest #${d.id}</span>
      <span>${when.toLocaleString()}</span>
      <span class="tag">${esc(d.engine === 'extractive' ? 'rule-based summaries' : d.engine || '')}</span>
      <span class="spacer"></span>
      <div class="menu">
        <button class="btn sm" id="share-btn" aria-haspopup="true">Share ▾</button>
        <div class="menu-pop" id="share-pop" hidden>
          <button data-share="copy">Copy as Markdown</button>
          <button data-share="open">Open .md file</button>
          <button data-share="slack" ${slackOk ? '' : 'disabled title="Set SLACK_WEBHOOK_URL to enable"'}>Send to Slack${slackOk ? '' : '<small>not set up</small>'}</button>
          <button data-share="email" ${mailOk ? '' : 'disabled title="Set SMTP_HOST and EMAIL_TO to enable"'}>Send by email${mailOk ? '' : '<small>not set up</small>'}</button>
        </div>
      </div>
    </div>
    <p class="headline">${esc(d.headline)}</p>
    <ul class="themes">${(d.themes || []).map((t, i) => `<li style="animation-delay:${i * 70}ms">${esc(t)}</li>`).join('')}</ul>
    ${(d.articles || []).length ? `
    <div class="toolbar">
      <div class="search">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
        <input id="q" placeholder="Search stories  ( / )" value="${esc(view.q)}">
      </div>
      <select id="sort" aria-label="Sort">
        <option value="relevance">Most relevant</option><option value="recent">Most recent</option><option value="source">By source</option>
      </select>
      <div class="seg">
        <button id="v-full" class="${document.body.classList.contains('compact') ? '' : 'on'}">Detailed</button>
        <button id="v-compact" class="${document.body.classList.contains('compact') ? 'on' : ''}">Compact</button>
      </div>
    </div>
    <div class="chips" id="chips">
      <span class="chip ${view.topic === 'all' ? 'on' : ''}" data-topic="all">All<span class="c">${d.articles.length}</span></span>
      ${Object.entries(topics).sort((a, b) => b[1] - a[1]).map(([t, c]) =>
        `<span class="chip ${view.topic === t ? 'on' : ''}" data-topic="${esc(t)}">${esc(t)}<span class="c">${c}</span></span>`).join('')}
      <span class="chip ${view.saved ? 'on' : ''}" id="chip-saved">★ Saved</span>
    </div>
    <div id="stories"></div>` : `<div class="empty"><h3>No stories cleared the filters</h3>
      <p>Lower the minimum score, widen the lookback window, or add feeds.</p>
      <button class="btn" onclick="openDrawer('filters')">Adjust filters</button></div>`}
    ${(d.errors || []).length ? `<p class="meta" style="margin-top:16px">Feeds that did not respond: ${d.errors.map(esc).join(', ')}</p>` : ''}`;

  $('share-btn').onclick = e => { e.stopPropagation(); $('share-pop').hidden = !$('share-pop').hidden; };
  $('share-pop').onclick = e => { const b = e.target.closest('[data-share]'); if (b && !b.disabled) { $('share-pop').hidden = true; share(b.dataset.share, b); } };
  if (!(d.articles || []).length) return;
  $('sort').value = view.sort;
  $('q').addEventListener('input', e => { view.q = e.target.value; view.focus = -1; renderStories(true); });
  $('sort').addEventListener('change', e => { view.sort = e.target.value; renderStories(); });
  $('v-full').onclick = () => setCompact(false);
  $('v-compact').onclick = () => setCompact(true);
  $('chips').addEventListener('click', e => {
    const chip = e.target.closest('.chip'); if (!chip) return;
    if (chip.id === 'chip-saved') { view.saved = !view.saved; chip.classList.toggle('on', view.saved); }
    else { view.topic = chip.dataset.topic; $('chips').querySelectorAll('[data-topic]').forEach(c => c.classList.toggle('on', c === chip)); }
    view.focus = -1; renderStories();
  });
  renderStories();
}
document.addEventListener('click', () => { const p = $('share-pop'); if (p) p.hidden = true; });

function renderStories(quiet) {
  const list = visibleStories(), box = $('stories');
  if (!list.length) { box.innerHTML = `<div class="empty">No stories match. <a href="#" onclick="clearFilters();return false">Clear filters</a></div>`; return; }
  box.innerHTML = list.map((a, n) => {
    const k = storyKey(a), isSaved = saved.has(k), isRead = readSet.has(k);
    return `<article class="story ${isRead ? 'read' : ''} ${n === view.focus ? 'focus' : ''}" data-k="${esc(k)}" data-n="${n}"
               style="${quiet ? 'animation:none' : `animation-delay:${Math.min(n, 8) * 60}ms`}">
      <div class="ring" style="--t:${a.relevance};--c:${relColor(a.relevance)}" title="Relevance ${a.relevance}/100"><span>${a.relevance}</span></div>
      <div>
        <h3><a href="${esc(a.link)}" target="_blank" rel="noopener" data-act="open">${esc(a.title)}</a>${isSaved ? '<span class="star" title="Saved">★</span>' : ''}</h3>
        <div class="meta"><span class="src">${avatar(a.source)}${esc(a.source)}</span>
          <span>${esc(a.age || '')}</span><span class="tag">${esc(a.topic)}</span></div>
        <ul>${(a.bullets || []).map(b => `<li>${esc(b)}</li>`).join('')}</ul>
        ${a.why_it_matters ? `<p class="why"><b>Why it matters:</b> ${esc(a.why_it_matters)}</p>` : ''}
        <div class="acts">
          <button class="btn sm ${isSaved ? 'on' : ''}" data-act="save">${isSaved ? '★ Saved' : '☆ Save'}</button>
          <button class="btn sm" data-act="read">${isRead ? 'Mark unread' : 'Mark read'}</button>
          <button class="btn sm" data-act="copy">Copy</button>
          <a class="btn sm" href="${esc(a.link)}" target="_blank" rel="noopener" data-act="open">Open ↗</a>
        </div>
      </div></article>`;
  }).join('');
}
function clearFilters() { view.q = ''; view.topic = 'all'; view.saved = false; renderDigest(); }
function setCompact(on) {
  document.body.classList.toggle('compact', on); store.set('compact', on);
  if ($('v-full')) { $('v-full').classList.toggle('on', !on); $('v-compact').classList.toggle('on', on); }
}
setCompact(store.get('compact', false));

document.addEventListener('click', e => {
  const btn = e.target.closest('[data-act]'), card = e.target.closest('.story');
  if (!card) return;
  view.focus = +card.dataset.n;
  document.querySelectorAll('.story').forEach(s => s.classList.toggle('focus', s === card));
  if (btn) storyAction(btn.dataset.act, card);
});
function storyAction(act, card) {
  const k = card.dataset.k, a = (current?.articles || []).find(x => storyKey(x) === k);
  if (!a) return;
  if (act === 'save') {
    saved.has(k) ? saved.delete(k) : saved.add(k); store.set('saved', [...saved]);
    toast(saved.has(k) ? 'Saved ★' : 'Removed from saved'); renderStories(true);
  } else if (act === 'read') {
    readSet.has(k) ? readSet.delete(k) : readSet.add(k); store.set('read', [...readSet].slice(-500)); renderStories(true);
  } else if (act === 'copy') {
    const text = `${a.title}\n${a.bullets.map(b => '• ' + b).join('\n')}\n${a.why_it_matters ? 'Why it matters: ' + a.why_it_matters + '\n' : ''}${a.link}`;
    navigator.clipboard.writeText(text).then(() => toast('Story copied.', 'good'), () => toast('Copy failed', 'bad'));
  } else if (act === 'open') {
    readSet.add(k); store.set('read', [...readSet].slice(-500)); setTimeout(() => renderStories(true), 300);
  }
}

async function share(what, btn) {
  if (!current) return;
  if (what === 'open') { window.open('/api/digest/' + current.id + '/markdown', '_blank'); return; }
  if (what === 'copy') {
    const text = await (await fetch('/api/digest/' + current.id + '/markdown')).text();
    navigator.clipboard.writeText(text).then(() => toast('Markdown copied.', 'good'),
      () => window.open('/api/digest/' + current.id + '/markdown', '_blank'));
    return;
  }
  toast('Sending…');
  const r = await api(`/api/deliver/${what}/${current.id}`, {method:'POST'});
  toast(r.message || 'Done.', r.ok ? 'good' : 'bad');
}

async function loadHistory() {
  const rows = await api('/api/history');
  $('hist-count').textContent = rows.length ? `${rows.length} run${rows.length > 1 ? 's' : ''}` : '';
  $('history').innerHTML = rows.length
    ? rows.map(r => `<li data-id="${r.id}" class="${current && current.id === r.id ? 'active' : ''}" onclick="showDigest(${r.id})">
        <span>#${r.id} · ${new Date(r.created_at).toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'})}</span>
        <span class="n">${r.article_count} stories</span></li>`).join('')
    : '<li><span class="sub">No runs yet</span></li>';
}

/* ---------------- settings drawer ---------------- */
function openDrawer(tab) { $('drawer').classList.add('show'); $('scrim').classList.add('show'); if (tab) showTab(tab); }
function closeDrawer() {
  if (dirty && !$('dirty').dataset.warned) { $('dirty').dataset.warned = 1; $('dirty').textContent = 'Unsaved changes — close again to discard'; return; }
  $('drawer').classList.remove('show'); $('scrim').classList.remove('show');
  if (dirty) loadSettings();
}
function showTab(name) {
  document.querySelectorAll('.tabs button').forEach(b => b.classList.toggle('on', b.dataset.tab === name));
  document.querySelectorAll('[data-pane]').forEach(p => p.hidden = p.dataset.pane !== name);
}
document.querySelectorAll('.tabs button').forEach(b => b.onclick = () => showTab(b.dataset.tab));
function markDirty() { dirty = true; delete $('dirty').dataset.warned; $('dirty').textContent = '● Unsaved changes'; }

function renderTags(boxId, inputId, list) {
  const box = $(boxId), input = $(inputId);
  box.querySelectorAll('.t').forEach(t => t.remove());
  list.forEach((v, i) => {
    const t = document.createElement('span'); t.className = 't';
    t.innerHTML = `${esc(v)}<button aria-label="Remove ${esc(v)}">×</button>`;
    t.querySelector('button').onclick = () => { list.splice(i, 1); renderTags(boxId, inputId, list); markDirty(); };
    box.insertBefore(t, input);
  });
}
function wireTags(boxId, inputId, getList) {
  const input = $(inputId);
  $(boxId).onclick = () => input.focus();
  input.addEventListener('keydown', e => {
    const list = getList();
    if ((e.key === 'Enter' || e.key === ',') && input.value.trim()) {
      e.preventDefault();
      input.value.split(',').map(v => v.trim()).filter(Boolean).forEach(v => { if (!list.includes(v)) list.push(v); });
      input.value = ''; renderTags(boxId, inputId, list); markDirty();
    } else if (e.key === 'Backspace' && !input.value && list.length) { list.pop(); renderTags(boxId, inputId, list); markDirty(); }
  });
}
wireTags('kw-box', 'kw-input', () => kw);
wireTags('ex-box', 'ex-input', () => ex);

function renderFeeds() {
  $('feed-count').textContent = `· ${feeds.length}`;
  $('feeds').innerHTML = feeds.map((f, i) => `<div class="feed"><input value="${esc(f)}" data-i="${i}" aria-label="Feed URL">
    <button class="btn sm" data-del="${i}" aria-label="Remove feed">✕</button></div>`).join('');
  $('feeds').querySelectorAll('input').forEach(inp => inp.oninput = () => { feeds[+inp.dataset.i] = inp.value.trim(); markDirty(); });
  $('feeds').querySelectorAll('[data-del]').forEach(b => b.onclick = () => { feeds.splice(+b.dataset.del, 1); renderFeeds(); markDirty(); });
}
function addFeed() {
  const v = $('feed-new').value.trim();
  if (!/^https?:\/\//.test(v)) { toast('Enter a full http(s) URL', 'bad'); return; }
  feeds.push(v); $('feed-new').value = ''; renderFeeds(); markDirty();
}
$('feed-new').addEventListener('keydown', e => { if (e.key === 'Enter') addFeed(); });

const RANGES = [['min_relevance', 'o-min', v => v], ['max_articles', 'o-max', v => v],
                ['lookback_hours', 'o-look', v => v >= 48 ? `${(v / 24).toFixed(v % 24 ? 1 : 0)}d` : `${v}h`]];
RANGES.forEach(([id, out, fmt]) => $(id).addEventListener('input', () => { $(out).textContent = fmt(+$(id).value); markDirty(); }));
$('profile').addEventListener('input', markDirty);
$('use_google_news').addEventListener('change', markDirty);
document.querySelectorAll('[data-preset]').forEach(b => b.onclick = () => { $('profile').value = PRESETS[b.dataset.preset]; markDirty(); });

async function loadSettings(notify) {
  settings = await api('/api/settings');
  $('profile').value = settings.profile || '';
  kw = [...(settings.keywords || [])]; ex = [...(settings.exclude || [])]; feeds = [...(settings.feeds || [])];
  renderTags('kw-box', 'kw-input', kw); renderTags('ex-box', 'ex-input', ex); renderFeeds();
  RANGES.forEach(([id, out, fmt]) => { $(id).value = settings[id]; $(out).textContent = fmt(+settings[id]); });
  $('use_google_news').checked = !!settings.use_google_news;
  dirty = false; $('dirty').textContent = '';
  const llm = $('pill-llm');
  llm.textContent = settings._llm_ready ? settings.model : 'Rule-based mode';
  llm.className = 'pill ' + (settings._llm_ready ? 'on' : 'off');
  llm.title = settings._llm_ready ? 'An AI model is writing the summaries'
    : 'No API key set: summaries are built with keyword and sentence rules. Set ANTHROPIC_API_KEY to use an AI model.';
  if (notify) toast('Reverted to saved settings.');
}
async function saveSettings() {
  const body = {
    profile: $('profile').value.trim(), keywords: kw, exclude: ex, feeds: feeds.filter(Boolean),
    lookback_hours: +$('lookback_hours').value, max_articles: +$('max_articles').value,
    min_relevance: +$('min_relevance').value, use_google_news: $('use_google_news').checked,
  };
  await api('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
  dirty = false; $('dirty').textContent = '';
  toast('Settings saved. They apply to the next run.', 'good');
}

/* ---------------- keyboard ---------------- */
function showKeys(on) { $('keys').hidden = !on; }
document.addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') {
    if ($('drawer').classList.contains('show')) { e.preventDefault(); saveSettings(); }
    return;
  }
  if (e.key === 'Escape') {
    showKeys(false); if ($('share-pop')) $('share-pop').hidden = true;
    if ($('drawer').classList.contains('show')) closeDrawer();
    else if (document.activeElement?.id === 'q') document.activeElement.blur();
    return;
  }
  if (/INPUT|TEXTAREA|SELECT/.test(document.activeElement?.tagName) || e.ctrlKey || e.metaKey || e.altKey) return;
  const cards = [...document.querySelectorAll('.story')], key = e.key.toLowerCase();
  if (key === 'r') runNow();
  else if (key === '/') { if ($('q')) { e.preventDefault(); $('q').focus(); } }
  else if (key === ',') openDrawer();
  else if (key === '?') showKeys(true);
  else if (key === 'c') setCompact(!document.body.classList.contains('compact'));
  else if ((key === 'j' || key === 'k') && cards.length) {
    view.focus = Math.max(0, Math.min(cards.length - 1, view.focus + (key === 'j' ? 1 : -1)));
    cards.forEach((c, i) => c.classList.toggle('focus', i === view.focus));
    cards[view.focus].scrollIntoView({block:'center', behavior:'smooth'});
  } else if (['o', 's', 'x'].includes(key) && cards[view.focus]) {
    const card = cards[view.focus];
    if (key === 'o') { window.open(card.querySelector('h3 a').href, '_blank'); storyAction('open', card); }
    else storyAction(key === 's' ? 'save' : 'read', card);
  }
});

/* ---------------- boot ---------------- */
$('btn-run').innerHTML = RUN_LABEL;
updatePipeline();
renderFunnel({});
loadSettings().then(() => showDigest(null)).then(loadHistory);
poll();
const _hash = location.hash.match(/^#configure(?:-(\w+))?/);   // deep link: #configure-filters
if (_hash) openDrawer(_hash[1]);
</script>
</body>
</html>
"""


# =========================================================================== #
#  ENTRYPOINT
# =========================================================================== #
def free_port(preferred: int) -> int:
    for candidate in range(preferred, preferred + 12):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", candidate))
                return candidate
            except OSError:
                continue
    return preferred


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily Industry News Summarizer Agent")
    parser.add_argument("--once", action="store_true",
                        help="run one digest headless, print the markdown, deliver, exit")
    parser.add_argument("--port", type=int, default=int(os.getenv("NEWS_AGENT_PORT", "8000")))
    parser.add_argument("--no-browser", action="store_true", help="do not open a browser")
    args = parser.parse_args()

    init_db()

    if args.once:
        digest = run_pipeline()
        print("\n" + "=" * 78 + "\n")
        print(digest_markdown(digest))
        for sender, label in ((send_to_slack, "Slack"), (send_email, "email")):
            try:
                print("\n" + sender(digest))
            except Exception as exc:
                print(f"\n(skipped {label}: {exc})")
        return

    port = free_port(args.port)
    url = f"http://127.0.0.1:{port}"
    client = ClaudeClient(log=lambda *_: None)

    print("\n" + "=" * 70)
    print("  Daily Industry News Summarizer Agent")
    print("=" * 70)
    print(f"  Dashboard : {url}")
    print(f"  Model     : {MODEL}" if client.available
          else f"  Model     : none ({client.reason}) -- extractive fallback")
    print(f"  Database  : {DB_PATH}")
    run_at = os.getenv("DAILY_RUN_AT", "").strip()
    if run_at:
        threading.Thread(target=scheduler_loop, args=(run_at,), daemon=True).start()
        print(f"  Scheduled : every day at {run_at} (local time)")
    print("=" * 70)
    print("  Open the link above in your browser. Ctrl+C to stop.\n")

    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    main()
````
