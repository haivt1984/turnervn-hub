from workers import WorkerEntrypoint, Response, fetch
from datetime import datetime, timezone
import json
import re
import hashlib
from html import unescape
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
import xml.etree.ElementTree as ET

"""
TURNER VIETNAM NEWS & KNOWLEDGE HUB
collector.py - single-file Cloudflare Python Worker

What it does:
- Reads active source definitions from the SOURCES list below.
- Uses RSS/Atom when a feed_url is provided.
- Falls back to simple HTML link discovery when feed_url is empty.
- Fetches article pages, extracts title/description/date/images/content.
- Deduplicates by canonical URL and content hash.
- Stores articles in Supabase Postgres via REST API.
- Downloads primary article images to Supabase Storage when possible.
- Logs each crawl to hub_crawl_logs.
- Exposes a health endpoint at /.
- Exposes a protected manual run endpoint:
      /?action=run&token=YOUR_COLLECTOR_TOKEN
- Runs automatically via Cloudflare Cron through scheduled().

REQUIRED CLOUDFLARE SECRETS:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
  COLLECTOR_TOKEN   (recommended for manual test endpoint)

IMPORTANT:
- Never put SERVICE ROLE KEY in index.html.
- Review each source's robots.txt, Terms of Service and copyright permissions.
- Prefer RSS/API where available. HTML fallback is intentionally conservative.
"""

# ============================================================
# CONFIGURATION
# ============================================================

USER_AGENT = (
    "TurnerVietnamNewsCollector/1.0 "
    "(+https://example.invalid/contact; respectful crawler)"
)
REQUEST_TIMEOUT_MS = 15000  # reserved for future AbortSignal-based timeout handling
MAX_ARTICLES_PER_SOURCE = 100
MAX_CONTENT_CHARS = 120000
MAX_DESCRIPTION_CHARS = 1000
MAX_TITLE_CHARS = 500
MAX_IMAGE_BYTES = 8 * 1024 * 1024
DISCOVERY_PAGES_PER_RUN = 1
PROCESS_BATCH_SIZE = 3

# Cloudflare Cron schedules are UTC.
# Example: 0 * * * * = every hour on the hour.
CRON_SCHEDULE = "0 * * * *"
BUILD_VERSION = "batch-v2.4-20260928"

# Add/edit sources here. For the most reliable ingestion, fill feed_url
# with an official RSS/Atom feed. When feed_url is empty, the collector
# performs conservative HTML discovery from page_url.
SOURCES = [
    # ---- Vietnam / official ----
    {
        "name": "Ministry of Construction",
        "page_url": "https://moc.gov.vn/",
        "discovery_url": "https://moc.gov.vn/vn/chuyen-muc/1205/tin-tuc.aspx",
        "feed_url": "",
        "pagination": {
            "enabled": True,
            "url_template": "https://moc.gov.vn/vn/Pages/chuyenmuctin.aspx?ChuyenmucID=1173&page={page}&tieude=tin-hoat-dong.aspx",
            "start_page": 1,
            "max_pages": 60
        },
        "feed_urls": [
            "https://moc.gov.vn/rss/1176/tin-chi-dao--dieu-hanh.rss",
            "https://moc.gov.vn/rss/1173/tin-hoat-dong.rss",
            "https://moc.gov.vn/rss/1184/tin-tong-hop.rss",
            "https://moc.gov.vn/rss/1196/gioi-thieu-van-ban-moi.rss",
            "https://moc.gov.vn/rss/1166/tin-cai-cach-hanh-chinh.rss",
            "https://moc.gov.vn/rss/1303/chien-luoc--quy-hoach--ke-hoach.rss",
            "https://moc.gov.vn/rss/1207/thong-tin---tu-lieu.rss"
        ],
        "category": "Vietnam & Regulation",
        "content_type": "NEWS",
        "active": True,
    },
    {
        "name": "Thu Vien Phap Luat",
        "page_url": "https://thuvienphapluat.vn/",
        "feed_url": "",
        "category": "Legal & Regulation",
        "content_type": "LEGAL",
        "active": True,
    },
    {
        "name": "Vietnam National Statistics Office",
        "page_url": "https://www.nso.gov.vn/",
        "feed_url": "",
        "category": "Vietnam Market",
        "content_type": "MARKET_INTELLIGENCE",
        "active": True,
    },

    # ---- Construction / technology ----
    {
        "name": "ENR",
        "page_url": "https://www.enr.com/",
        "feed_url": "",
        "category": "Construction",
        "content_type": "NEWS",
        "active": True,
    },
    {
        "name": "Construction Dive",
        "page_url": "https://www.constructiondive.com/",
        "feed_url": "",
        "category": "Construction",
        "content_type": "NEWS",
        "active": True,
    },
    {
        "name": "Procore",
        "page_url": "https://www.procore.com/blog",
        "feed_url": "",
        "category": "Construction Technology",
        "content_type": "UPDATE",
        "active": True,
    },
    {
        "name": "OpenSpace",
        "page_url": "https://www.openspace.ai/blog/",
        "feed_url": "",
        "category": "Construction Technology",
        "content_type": "UPDATE",
        "active": True,
    },
    {
        "name": "DroneDeploy",
        "page_url": "https://www.dronedeploy.com/blog",
        "feed_url": "",
        "category": "Construction Technology",
        "content_type": "UPDATE",
        "active": True,
    },
    {
        "name": "HoloBuilder / FARO",
        "page_url": "https://www.holobuilder.com/",
        "feed_url": "",
        "category": "Construction Technology",
        "content_type": "UPDATE",
        "active": True,
    },
    {
        "name": "Autodesk Construction",
        "page_url": "https://www.autodesk.com/blogs/construction/",
        "feed_url": "",
        "category": "Construction Technology",
        "content_type": "NEWS",
        "active": True,
    },

    # ---- Microsoft 365 ----
    {
        "name": "Microsoft SharePoint",
        "page_url": "https://techcommunity.microsoft.com/category/SharePoint/blog/SharePoint",
        "feed_url": "",
        "category": "Microsoft 365",
        "content_type": "NEWS",
        "active": True,
    },
    {
        "name": "Microsoft Teams",
        "page_url": "https://techcommunity.microsoft.com/category/MicrosoftTeams/blog/MicrosoftTeamsBlog",
        "feed_url": "",
        "category": "Microsoft 365",
        "content_type": "NEWS",
        "active": True,
    },
    {
        "name": "Microsoft Planner",
        "page_url": "https://techcommunity.microsoft.com/t5/planner-blog/bg-p/PlannerBlog",
        "feed_url": "",
        "category": "Microsoft 365",
        "content_type": "NEWS",
        "active": True,
    },
    {
        "name": "Microsoft Power BI",
        "page_url": "https://powerbi.microsoft.com/blog/",
        "feed_url": "",
        "category": "Microsoft 365",
        "content_type": "NEWS",
        "active": True,
    },
    {
        "name": "Microsoft Power Platform",
        "page_url": "https://www.microsoft.com/en-us/power-platform/blog/",
        "feed_url": "",
        "category": "Microsoft 365",
        "content_type": "NEWS",
        "active": True,
    },

    # ---- Turner ----
    {
        "name": "Turner Insights",
        "page_url": "https://www.turnerconstruction.com/insights",
        "feed_url": "",
        "category": "Turner Global",
        "content_type": "TURNER_INTERNAL",
        "active": True,
    },
]

# ============================================================
# GENERIC HELPERS
# ============================================================


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def clean_text(value):
    if not value:
        return ""
    value = unescape(str(value))
    value = re.sub(r"<script[\s\S]*?</script>", " ", value, flags=re.I)
    value = re.sub(r"<style[\s\S]*?</style>", " ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def truncate(value, n):
    value = value or ""
    return value if len(value) <= n else value[: n - 1].rstrip() + "…"


def sha256_text(value):
    return hashlib.sha256((value or "").encode("utf-8", "ignore")).hexdigest()


def canonicalize_url(url):
    if not url:
        return ""
    try:
        p = urlparse(url)
        if not p.scheme or not p.netloc:
            return url
        # Remove obvious tracking query parameters.
        keep = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            lk = k.lower()
            if lk.startswith("utm_") or lk in {"fbclid", "gclid", "mc_cid", "mc_eid"}:
                continue
            keep.append((k, v))
        query = urlencode(keep)
        path = re.sub(r"//+", "/", p.path or "/")
        if path != "/":
            path = path.rstrip("/")
        return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", query, ""))
    except Exception:
        return url


def parse_date(value):
    if not value:
        return None
    value = value.strip()
    # ISO variants.
    try:
        v = value.replace("Z", "+00:00")
        d = datetime.fromisoformat(v)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc).isoformat()
    except Exception:
        pass

    # Common RSS date shape: RFC 822.
    try:
        from email.utils import parsedate_to_datetime
        d = parsedate_to_datetime(value)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def find_first(patterns, text, flags=re.I | re.S):
    for pattern in patterns:
        m = re.search(pattern, text or "", flags)
        if m:
            return clean_text(m.group(1))
    return ""


def html_entities_to_text(value):
    return clean_text(value)


async def http_get(url, accept="*/*"):
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": accept,
        "Cache-Control": "no-cache",
    }
    return await fetch(
        url,
        headers=headers,
        redirect="follow",
    )


async def response_text(resp):
    return await resp.text()


# ============================================================
# SUPABASE REST / STORAGE
# ============================================================


def sb_headers(env, content_type="application/json"):
    return {
        "apikey": env.SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": "Bearer " + env.SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": content_type,
        "Accept": "application/json",
    }


def sb_url(env, path):
    return env.SUPABASE_URL.rstrip("/") + path


async def sb_request(env, method, path, body=None, extra_headers=None):
    headers = sb_headers(env)
    if extra_headers:
        headers.update(extra_headers)
    if body is not None:
        return await fetch(
            sb_url(env, path),
            method=method,
            headers=headers,
            body=json.dumps(body),
        )
    return await fetch(
        sb_url(env, path),
        method=method,
        headers=headers,
    )


async def get_or_create_source(env, source):
    name = source["name"]
    url = source["page_url"]
    query = "?select=id,name,url,feed_url,category,active&url=eq." + url.replace(" ", "%20")
    resp = await sb_request(env, "GET", "/rest/v1/hub_sources" + query)
    if resp.ok:
        data = json.loads(await resp.text())
        if data:
            return data[0]["id"]

    body = {
        "name": name,
        "url": url,
        "feed_url": source.get("feed_url") or None,
        "category": source.get("category") or "General",
        "crawl_method": "rss" if source.get("feed_url") else "html",
        "active": bool(source.get("active", True)),
        "created_at": now_iso(),
    }
    resp = await sb_request(env, "POST", "/rest/v1/hub_sources", body, {"Prefer": "return=representation"})
    if not resp.ok:
        raise RuntimeError("Supabase source insert failed: " + await resp.text())
    data = json.loads(await resp.text())
    return data[0]["id"]


async def article_exists(env, canonical_url, content_hash):
    path = "/rest/v1/hub_articles?select=id,canonical_url&or="
    # PostgREST or syntax: (canonical_url.eq.X,content_hash.eq.Y)
    # URL-encode manually for safe basic use.
    encoded_url = canonical_url.replace(",", "%2C").replace(" ", "%20")
    path += "(canonical_url.eq." + encoded_url + ",content_hash.eq." + content_hash + ")"
    resp = await sb_request(env, "GET", path)
    if not resp.ok:
        return False
    data = json.loads(await resp.text())
    return bool(data)


async def insert_article(env, article):
    resp = await sb_request(
        env,
        "POST",
        "/rest/v1/hub_articles",
        article,
        {"Prefer": "return=representation,resolution=ignore-duplicates"},
    )
    if resp.ok:
        data = json.loads(await resp.text())
        if data:
            return data[0].get("id")
        return None
    raise RuntimeError("Supabase article insert failed: " + await resp.text())


async def insert_image_record(env, image_record):
    resp = await sb_request(
        env,
        "POST",
        "/rest/v1/hub_article_images",
        image_record,
        {"Prefer": "return=minimal"},
    )
    if not resp.ok:
        raise RuntimeError("Supabase image record insert failed: " + await resp.text())


async def insert_log(env, log_record):
    try:
        await sb_request(env, "POST", "/rest/v1/hub_crawl_logs", log_record, {"Prefer": "return=minimal"})
    except Exception as exc:
        print("crawl log error:", str(exc))


async def update_source_last_crawled(env, source_id):
    path = "/rest/v1/hub_sources?id=eq." + str(source_id)
    try:
        await sb_request(
            env,
            "PATCH",
            path,
            {"last_crawled_at": now_iso()},
            {"Prefer": "return=minimal"},
        )
    except Exception as exc:
        print("source update error:", str(exc))


async def upload_storage(env, bucket, path, content_bytes, mime_type):
    url = sb_url(env, "/storage/v1/object/" + bucket + "/" + path)
    headers = {
        "apikey": env.SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": "Bearer " + env.SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": mime_type or "application/octet-stream",
        "Cache-Control": "public, max-age=31536000, immutable",
        "x-upsert": "true",
    }
    resp = await fetch(
        url,
        method="POST",
        headers=headers,
        body=content_bytes,
    )
    if not resp.ok:
        raise RuntimeError("Storage upload failed: " + await resp.text())
    return sb_url(env, "/storage/v1/object/public/" + bucket + "/" + path)


# ============================================================
# RSS / ATOM PARSER
# ============================================================


def xml_local_name(tag):
    return tag.split("}")[-1].lower()


def child_text(node, names):
    names = {n.lower() for n in names}
    for child in list(node):
        if xml_local_name(child.tag) in names:
            text = "".join(child.itertext())
            return clean_text(text)
    return ""


def child_link(node):
    # RSS <link> text or Atom <link href="...">
    for child in list(node):
        if xml_local_name(child.tag) != "link":
            continue
        href = child.attrib.get("href")
        if href:
            rel = child.attrib.get("rel", "alternate")
            if rel in {"alternate", "canonical"}:
                return href.strip()
        text = "".join(child.itertext()).strip()
        if text:
            return text
    return ""


def parse_feed(xml_text, base_url, source):
    items = []
    try:
        root = ET.fromstring(xml_text)
    except Exception as exc:
        print("RSS parse error:", source["name"], str(exc))
        return items

    for node in root.iter():
        name = xml_local_name(node.tag)
        if name not in {"item", "entry"}:
            continue

        title = child_text(node, ["title"])
        url = child_link(node)
        description = child_text(node, ["description", "summary", "content", "encoded"])
        author = child_text(node, ["creator", "author", "name"])
        date_value = child_text(node, ["pubdate", "published", "updated", "date"])
        image_url = ""

        # Common media/content namespaces.
        for child in list(node):
            local = xml_local_name(child.tag)
            if local in {"content", "thumbnail", "enclosure"}:
                candidate = child.attrib.get("url") or child.attrib.get("href")
                if candidate:
                    image_url = candidate
                    break

        if not title or not url:
            continue

        url = canonicalize_url(urljoin(base_url, url))
        image_url = urljoin(base_url, image_url) if image_url else ""

        items.append({
            "title": truncate(title, MAX_TITLE_CHARS),
            "url": url,
            "description": truncate(description, MAX_DESCRIPTION_CHARS),
            "author": truncate(author, 200),
            "published_at": parse_date(date_value) or now_iso(),
            "image_url": image_url,
            "category": source.get("category", "General"),
            "content_type": source.get("content_type", "NEWS"),
        })

    return items[:MAX_ARTICLES_PER_SOURCE]


# ============================================================
# HTML DISCOVERY / ARTICLE EXTRACTION
# ============================================================


def strip_comments(html):
    return re.sub(r"<!--.*?-->", " ", html, flags=re.S)


def html_meta(html, names):
    for name in names:
        p1 = rf'<meta[^>]+(?:name|property)=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\'][^>]*>'
        p2 = rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:name|property)=["\']{re.escape(name)}["\'][^>]*>'
        val = find_first([p1, p2], html)
        if val:
            return unescape(val)
    return ""


def extract_title(html):
    title = html_meta(html, ["og:title", "twitter:title"])
    if title:
        return truncate(clean_text(title), MAX_TITLE_CHARS)
    return truncate(find_first([r"<h1[^>]*>(.*?)</h1>", r"<title[^>]*>(.*?)</title>"], html), MAX_TITLE_CHARS)


def extract_description(html):
    value = html_meta(html, ["description", "og:description", "twitter:description"])
    return truncate(clean_text(value), MAX_DESCRIPTION_CHARS)


def extract_published(html):
    value = html_meta(html, ["article:published_time", "datePublished", "publishdate", "pubdate"])
    if value:
        return parse_date(value)
    return parse_date(find_first([
        r'<time[^>]+datetime=["\']([^"\']+)["\'][^>]*>',
        r'<meta[^>]+itemprop=["\']datePublished["\'][^>]+content=["\']([^"\']+)["\']',
    ], html))


def extract_author(html):
    value = html_meta(html, ["author", "article:author"])
    return truncate(clean_text(value), 200)


def extract_image(html, base_url):
    candidates = []

    value = html_meta(html, ["og:image", "twitter:image"])
    if value:
        candidates.append(urljoin(base_url, value))

    for m in re.finditer(r'<img[^>]+(?:src|data-src)=["\']([^"\']+)["\']', html, flags=re.I):
        candidates.append(urljoin(base_url, m.group(1)))
        if len(candidates) >= 20:
            break

    bad_tokens = (
        "logo", "icon", "favicon", "avatar", "placeholder",
        "sprite", "loading", "captcha"
    )

    for candidate in candidates:
        path = urlparse(candidate).path.lower()
        if any(token in path for token in bad_tokens):
            continue
        if any(path.endswith(ext) for ext in (".svg", ".gif", ".ico")):
            continue
        return candidate

    return ""


def extract_moc_published(html):
    # MOC article pages expose the publication timestamp in visible text as
    # DD/MM/YYYY HH:MM. The global site date has no time component, so this
    # avoids confusing the header date with the article publication date.
    page_text = clean_text(unescape(html))
    matches = re.findall(
        r'(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})'
        r'\s+(\d{1,2})\s*:\s*(\d{2})',
        page_text,
        flags=re.I,
    )

    for values in reversed(matches):
        day, month, year, hour, minute = [int(x) for x in values]
        try:
            return datetime(
                year, month, day, hour, minute,
                tzinfo=timezone(timedelta(hours=7)),
            ).isoformat()
        except Exception:
            continue

    return None


def extract_moc_main_text(html):
    candidates = []
    for ident in ("divArticleDescription1", "divArticleDescription2", "divArticleDescription3"):
        value = find_first([
            rf'<div[^>]+id=["\']{ident}["\'][^>]*>([\s\S]*?)</div>',
        ], html)
        text = clean_text(value)
        if len(text) > 80:
            candidates.append(text)

    # MOC's main article description is normally in description1/2/3.
    content = " ".join(dict.fromkeys(candidates))
    if len(content) >= 100:
        return truncate(content, MAX_CONTENT_CHARS)

    value = find_first([
        r'<div[^>]+class=["\'][^"\']*Around_News_Content[^"\']*["\'][^>]*>([\s\S]*?)</div>',
    ], html)
    return truncate(clean_text(value), MAX_CONTENT_CHARS)


def extract_moc_image(html, base_url):
    blocks = []
    for ident in ("divArticleDescription1", "divArticleDescription2", "divArticleDescription3"):
        block = find_first([
            rf'<div[^>]+id=["\']{ident}["\'][^>]*>([\s\S]*?)</div>',
        ], html)
        if block:
            blocks.append(block)

    content_html = " ".join(blocks)
    candidates = []

    for m in re.finditer(
        r'<img[^>]+(?:src|data-src)=["\']([^"\']+)["\']',
        content_html,
        flags=re.I,
    ):
        candidates.append(urljoin(base_url, unescape(m.group(1))))

    for candidate in candidates:
        path = urlparse(candidate).path.lower()
        if any(x in path for x in (
            "logo", "keyword", "icon", "favicon", "loading",
            "sprite", "avatar", "_layouts/images"
        )):
            continue
        if any(path.endswith(x) for x in (".svg", ".ico", ".gif")):
            continue
        return candidate

    # No valid article image is better than storing a site UI icon.
    return None


def extract_main_text(html):
    # Prefer article/main containers; strip obvious navigation/footer blocks.
    candidate = ""
    patterns = [
        r"<article[^>]*>([\s\S]*?)</article>",
        r"<main[^>]*>([\s\S]*?)</main>",
        r'<div[^>]+(?:class|id)=["\'][^"\']*(?:article|post|content|entry)[^"\']*["\'][^>]*>([\s\S]*?)</div>',
    ]
    for pattern in patterns:
        m = re.search(pattern, html, flags=re.I)
        if m:
            candidate = m.group(1)
            break
    if not candidate:
        candidate = html

    candidate = re.sub(r"<(nav|header|footer|aside|script|style|noscript|form)[^>]*>[\s\S]*?</\1>", " ", candidate, flags=re.I)
    text = clean_text(candidate)
    return truncate(text, MAX_CONTENT_CHARS)


def discover_moc_article_links(html, base_url):
    """
    Robust MOC-specific discovery. MOC may return article links as:
      /vn/Pages/chitiettin.aspx?ChuyenmucID=1173&IDNews=...
    or newer /vn/tin-tuc/.../*.aspx routes.
    """
    links = []
    seen = set()

    # First, inspect all href attribute values.
    href_values = re.findall(r"""href\s*=\s*["']([^"']+)["']""", html, flags=re.I)

    # MOC article URLs are discovered from href attributes.
    # Keep this parser deliberately simple to avoid regex portability issues.
    for value in href_values:
        value = unescape(value).strip()
        if not value:
            continue

        href = urljoin(base_url, value)
        if not href.startswith(("http://", "https://")):
            continue

        parsed = urlparse(href)
        host = parsed.netloc.lower()
        if host not in {"moc.gov.vn", "www.moc.gov.vn"}:
            continue

        path = parsed.path.lower()
        query = unescape(parsed.query).lower()

        is_detail = (
            (path.endswith("/chitiettin.aspx") and "idnews=" in query)
            or ("/vn/tin-tuc/" in path and path.endswith(".aspx"))
        )

        if not is_detail:
            continue

        href = canonicalize_url(href)
        if href in seen:
            continue

        seen.add(href)
        links.append((href, ""))

        if len(links) >= MAX_ARTICLES_PER_SOURCE:
            break

    return links


def discover_html_links(html, base_url, source=None):
    links = []
    seen = set()

    if source and source.get("name") == "Ministry of Construction":
        # Match relative or absolute article links; validate the resolved URL below.
        pattern = r'<a[^>]+href=["\']([^"\']+?\.aspx(?:\?[^"\']*)?)["\'][^>]*>([\s\S]*?)</a>'
    else:
        pattern = r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([\s\S]*?)</a>'

    for m in re.finditer(pattern, html, flags=re.I):
        href = urljoin(base_url, m.group(1))
        label = clean_text(m.group(2))

        if not href.startswith(("http://", "https://")):
            continue

        href = canonicalize_url(href)

        if source and source.get("name") == "Ministry of Construction":
            resolved_path = urlparse(href).path.lower()
            if "/vn/tin-tuc/" not in resolved_path:
                continue

        if href in seen:
            continue
        seen.add(href)

        path = urlparse(href).path.lower()
        if any(path.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".svg", ".pdf", ".zip")):
            continue

        if any(x in href.lower() for x in (
            "/login", "/signup", "/privacy", "/terms", "/search",
            "/contact", "/rss", "/sitemap"
        )):
            continue

        if len(label) < 15:
            continue

        links.append((href, label))

        if len(links) >= MAX_ARTICLES_PER_SOURCE:
            break

    return links


async def fetch_html_candidate(url, source):
    original_url = url
    fetch_url = url

    # MOC has two URL formats. Some /vn/tin-tuc/... URLs can return HTTP 500
    # from automated clients, while the site's legacy detail endpoint works.
    if source.get("name") == "Ministry of Construction":
        parsed = urlparse(url)
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 4 and parts[0].lower() == "vn" and parts[1].lower() == "tin-tuc":
            category_id = parts[2]
            news_id = parts[3]
            slug = parts[4] if len(parts) >= 5 else ""
            if category_id.isdigit() and news_id.isdigit():
                legacy_base = urljoin(
                    url,
                    "/vn/Pages/chitiettin.aspx"
                )
                fetch_url = (
                    legacy_base
                    + "?ChuyenmucID=" + category_id
                    + "&IDNews=" + news_id
                    + "&tieude=" + slug
                )

    resp = await http_get(fetch_url, "text/html,application/xhtml+xml")
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status}")

    html = await response_text(resp)
    is_moc = source.get("name") == "Ministry of Construction"

    return {
        "title": extract_title(html),
        "url": canonicalize_url(original_url),
        "description": extract_description(html),
        "author": extract_author(html),
        "published_at": extract_moc_published(html) if is_moc else extract_published(html),
        "image_url": extract_moc_image(html, fetch_url) if is_moc else extract_image(html, fetch_url),
        "content": extract_moc_main_text(html) if is_moc else extract_main_text(html),
        "category": source.get("category", "General"),
        "content_type": source.get("content_type", "NEWS"),
    }

async def discover_feed_url(source, html=None):
    """
    Discover an RSS/Atom feed advertised by the source page.
    Checks HTML <link> declarations first, then a few conventional paths.
    """
    base = source.get("page_url", "")
    if html:
        for m in re.finditer(
            r'<link[^>]+(?:type=["\']application/(?:rss|atom)\+xml["\'][^>]+href|href=["\'][^"\']+["\'][^>]+type=["\']application/(?:rss|atom)\+xml["\'])[^>]*>',
            html,
            flags=re.I,
        ):
            tag = m.group(0)
            href = re.search(r'href=["\']([^"\']+)["\']', tag, flags=re.I)
            if href:
                return canonicalize_url(urljoin(base, href.group(1)))

    candidates = [
        urljoin(base, "/rss.xml"),
        urljoin(base, "/feed/"),
        urljoin(base, "/feed.xml"),
        urljoin(base, "/rss/"),
        urljoin(base, "/atom.xml"),
    ]

    # Microsoft and common blog conventions.
    parsed = urlparse(base)
    host_root = f"{parsed.scheme}://{parsed.netloc}"
    candidates.extend([
        urljoin(host_root, "/feed/"),
        urljoin(host_root, "/rss.xml"),
    ])

    for candidate in candidates:
        try:
            resp = await http_get(candidate, "application/rss+xml,application/atom+xml,text/xml,*/*")
            if resp.ok:
                text = await response_text(resp)
                if "<rss" in text[:1000].lower() or "<feed" in text[:1000].lower():
                    return canonicalize_url(candidate)
        except Exception:
            continue

    return ""

async def crawl_html_source(source):
    results = []
    discovery_url = source.get("discovery_url") or source["page_url"]
    resp = await http_get(discovery_url, "text/html,application/xhtml+xml")
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status} for {discovery_url}")
    html = await response_text(resp)

    # Prefer an advertised RSS/Atom feed where available.
    discovered_feed = await discover_feed_url(source, html)
    if discovered_feed:
        try:
            feed_items = await crawl_rss_source({
                **source,
                "feed_url": discovered_feed,
            })
            if feed_items:
                return feed_items[:MAX_ARTICLES_PER_SOURCE]
        except Exception as exc:
            print("discovered RSS failed:", source["name"], discovered_feed, str(exc))

    if source.get("name") == "Ministry of Construction":
        links = discover_moc_article_links(html, discovery_url)
    else:
        links = discover_html_links(html, discovery_url, source)

    for url, label in links:
        try:
            item = await fetch_html_candidate(url, source)
            # Some sites hide article titles from generic extraction.
            if not item["title"]:
                item["title"] = truncate(label, MAX_TITLE_CHARS)
            if item["title"]:
                results.append(item)
        except Exception as exc:
            print("article fetch failed:", source["name"], url, str(exc))

        if len(results) >= MAX_ARTICLES_PER_SOURCE:
            break

    return results


async def crawl_rss_source(source):
    resp = await http_get(source["feed_url"], "application/rss+xml,application/atom+xml,text/xml,*/*")
    if not resp.ok:
        raise RuntimeError(f"RSS HTTP {resp.status}")
    xml_text = await response_text(resp)
    items = parse_feed(xml_text, source["page_url"], source)

    # Enrich RSS items by fetching the article page for content/images.
    enriched = []
    for item in items:
        try:
            if item["url"]:
                page = await fetch_html_candidate(item["url"], source)
                if page.get("title"):
                    item["title"] = page["title"]
                if page.get("description"):
                    item["description"] = page["description"]
                if page.get("author"):
                    item["author"] = page["author"]
                if page.get("published_at"):
                    # Prefer RSS published date when valid; only use page if needed.
                    pass
                if page.get("image_url"):
                    item["image_url"] = page["image_url"]
                item["content"] = page.get("content", "")
        except Exception as exc:
            print("RSS article enrichment failed:", source["name"], str(exc))
            item["content"] = item.get("description", "")
        enriched.append(item)
    return enriched[:MAX_ARTICLES_PER_SOURCE]


# ============================================================
# IMAGE HANDLING
# ============================================================


def extension_from_content_type(mime):
    m = (mime or "").lower().split(";")[0].strip()
    return {
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
        "image/gif": "gif",
        "image/avif": "avif",
    }.get(m, "bin")


async def store_primary_image(env, article_id, article_url, image_url):
    if not image_url:
        return "", "no_image_url"
    try:
        image_url = canonicalize_url(image_url)
        resp = await http_get(image_url, "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8")
        if not resp.ok:
            return "", f"image_http_{resp.status}"
        content_type = resp.headers.get("content-type", "application/octet-stream")
        if not content_type.lower().startswith("image/"):
            return "", "not_an_image"

        content = await resp.bytes()

        if len(content) == 0:
            return "", "empty_image"

        if len(content) > MAX_IMAGE_BYTES:
            return "", "image_too_large"

        ext = extension_from_content_type(content_type)
        safe_hash = sha256_text(image_url)[:16]
        path = f"{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/article-{article_id}/{safe_hash}.{ext}"
        public_url = await upload_storage(env, "hub-article-images", path, content, content_type)

        await insert_image_record(env, {
            "article_id": article_id,
            "original_url": image_url,
            "storage_path": path,
            "filename": path.split("/")[-1],
            "is_primary": True,
            "created_at": now_iso(),
        })
        return public_url, None
    except Exception as exc:
        print("image store failed:", image_url, str(exc))
        return "", str(exc)


# ============================================================
# ARTICLE PROCESSING
# ============================================================


def build_article_record(source, source_id, item):
    title = truncate(clean_text(item.get("title")), MAX_TITLE_CHARS)
    url = canonicalize_url(item.get("url"))
    description = truncate(clean_text(item.get("description")), MAX_DESCRIPTION_CHARS)
    content = truncate(clean_text(item.get("content")), MAX_CONTENT_CHARS)
    content_hash = sha256_text((title + "\n" + content)[:MAX_CONTENT_CHARS])

    return {
        "source_id": source_id,
        "title": title,
        "url": url,
        "canonical_url": url,
        "description": description,
        "content": content,
        "author": truncate(clean_text(item.get("author")), 200),
        "published_at": item.get("published_at"),
        "crawled_at": now_iso(),
        "category": item.get("category") or source.get("category") or "General",
        "image_url": item.get("image_url") or None,
        "ai_summary": None,
        "relevance": "medium",
        "content_hash": content_hash,
        "created_at": now_iso(),
        # These two are harmless when the DB ignores unknown fields only if
        # your schema includes them. Remove them if your schema is strict.
        # They are therefore NOT included here to match the baseline index.html.
    }


async def process_item(env, source, source_id, item):
    record = build_article_record(source, source_id, item)
    if not record["title"] or not record["canonical_url"]:
        return {"status": "skip", "reason": "missing title/url"}

    if await article_exists(env, record["canonical_url"], record["content_hash"]):
        return {"status": "duplicate"}

    article_id = await insert_article(env, record)
    stored_image_url = ""
    image_error = None

    if article_id and record.get("image_url"):
        stored_image_url, image_error = await store_primary_image(
            env,
            article_id,
            record["canonical_url"],
            record["image_url"],
        )
        if stored_image_url:
            patch_path = "/rest/v1/hub_articles?id=eq." + str(article_id)
            try:
                await sb_request(
                    env,
                    "PATCH",
                    patch_path,
                    {"image_url": stored_image_url},
                    {"Prefer": "return=minimal"},
                )
            except Exception as exc:
                print("article image URL update failed:", str(exc))

    return {
        "status": "new",
        "article_id": article_id,
        "stored_image_url": stored_image_url,
        "image_error": image_error,
    }


# ============================================================
# ARTICLE QUEUE
# ============================================================

async def queue_articles_bulk(env, source_id, urls):
    clean_urls = []
    seen = set()
    for url in urls:
        key = canonicalize_url(url)
        if not key or key in seen:
            continue
        seen.add(key)
        clean_urls.append(key)

    if not clean_urls:
        return 0

    rows = [{
        "source_id": source_id,
        "url": url,
        "status": "queued",
        "attempts": 0,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    } for url in clean_urls]

    # PostgREST needs the conflict target explicitly for reliable bulk
    # ignore-duplicates behavior with our unique URL index.
    resp = await sb_request(
        env,
        "POST",
        "/rest/v1/hub_article_queue?on_conflict=url",
        rows,
        {"Prefer": "resolution=ignore-duplicates,return=representation"},
    )
    if not resp.ok:
        raise RuntimeError("Bulk queue insert failed: " + await resp.text())

    inserted = json.loads(await resp.text() or "[]")
    return len(inserted)


async def queue_article(env, source_id, url):
    url = canonicalize_url(url)
    if not url:
        return False
    body = {
        "source_id": source_id,
        "url": url,
        "status": "queued",
        "attempts": 0,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }
    resp = await sb_request(
        env, "POST", "/rest/v1/hub_article_queue",
        body, {"Prefer": "resolution=ignore-duplicates,return=minimal"}
    )
    return resp.ok


async def get_queue_batch(env, source_id, limit=PROCESS_BATCH_SIZE):
    path = (
        "/rest/v1/hub_article_queue"
        "?select=id,source_id,url,status,attempts"
        f"&source_id=eq.{source_id}"
        "&status=eq.queued"
        "&order=id.asc"
        f"&limit={int(limit)}"
    )
    resp = await sb_request(env, "GET", path)
    if not resp.ok:
        raise RuntimeError("Queue read failed: " + await resp.text())
    return await resp.json()


async def update_queue_item(env, queue_id, values):
    values["updated_at"] = now_iso()
    path = "/rest/v1/hub_article_queue?id=eq." + str(queue_id)
    resp = await sb_request(env, "PATCH", path, values, {"Prefer": "return=minimal"})
    if not resp.ok:
        raise RuntimeError("Queue update failed: " + await resp.text())


async def discover_moc_batch(env, source, start_page=1, pages=DISCOVERY_PAGES_PER_RUN):
    source_id = await get_or_create_source(env, source)
    config = source.get("pagination") or {}
    template = config.get("url_template", "")
    discovered = 0
    queued = 0
    queue_errors = []
    page_stats = []

    for page_number in range(start_page, start_page + pages):
        page_url = template.format(page=page_number)
        resp = await http_get(page_url, "text/html,application/xhtml+xml")
        if not resp.ok:
            page_stats.append({"page": page_number, "status": resp.status, "links": 0})
            continue

        html = await response_text(resp)
        links = discover_moc_article_links(html, page_url)
        page_stats.append({"page": page_number, "status": resp.status, "links": len(links)})
        discovered += len(links)

        try:
            queued += await queue_articles_bulk(
                env,
                source_id,
                [url for url, _ in links],
            )
        except Exception as exc:
            queue_errors.append(str(exc))
            print("bulk queue failed:", source["name"], str(exc))

    return {
        "source": source["name"],
        "start_page": start_page,
        "pages": pages,
        "discovered": discovered,
        "queued": queued,
        "already_queued": max(discovered - queued, 0),
        "queue_errors": queue_errors,
        "page_stats": page_stats,
    }


async def process_queue_batch(env, source, limit=PROCESS_BATCH_SIZE):
    source_id = await get_or_create_source(env, source)
    rows = await get_queue_batch(env, source_id, limit)
    done = 0
    failed = 0
    new_count = 0
    duplicates = 0
    images_stored = 0
    item_errors = []

    for row in rows:
        qid = row["id"]
        attempts = int(row.get("attempts") or 0) + 1
        try:
            await update_queue_item(env, qid, {"status": "processing", "attempts": attempts})
            item = await fetch_html_candidate(row["url"], source)
            result = await process_item(env, source, source_id, item)

            if result["status"] == "new":
                new_count += 1
                if result.get("stored_image_url"):
                    images_stored += 1
            elif result["status"] == "duplicate":
                duplicates += 1

            await update_queue_item(env, qid, {
                "status": "done",
                "processed_at": now_iso(),
                "last_error": None,
            })
            done += 1
        except Exception as exc:
            message = truncate(str(exc), 2000)
            next_status = "failed" if attempts >= 3 else "queued"
            await update_queue_item(env, qid, {
                "status": next_status,
                "last_error": message,
            })
            item_errors.append({
                "queue_id": qid,
                "url": row["url"],
                "attempts": attempts,
                "status": next_status,
                "error": message,
            })
            failed += 1

    return {
        "source": source["name"],
        "selected": len(rows),
        "done": done,
        "failed": failed,
        "new": new_count,
        "duplicates": duplicates,
        "images_stored": images_stored,
        "item_errors": item_errors,
    }

# ============================================================
# SOURCE CRAWLING
# ============================================================


async def crawl_multi_feed_source(source):
    combined = []
    seen = set()

    for feed_url in source.get("feed_urls", []):
        try:
            feed_source = {**source, "feed_url": feed_url}
            items = await crawl_rss_source(feed_source)
            for item in items:
                key = canonicalize_url(item.get("url", ""))
                if not key or key in seen:
                    continue
                seen.add(key)
                combined.append(item)
        except Exception as exc:
            print("feed failed:", source["name"], feed_url, str(exc))

    def sort_key(item):
        try:
            return datetime.fromisoformat((item.get("published_at") or "").replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0

    combined.sort(key=sort_key, reverse=True)
    return combined[:MAX_ARTICLES_PER_SOURCE]


async def crawl_pagination_source(source):
    config = source.get("pagination") or {}
    if not config.get("enabled"):
        return []

    results = []
    seen = set()
    pages_visited = 0
    raw_links_seen = 0
    start_page = int(config.get("start_page", 1))
    max_pages = int(config.get("max_pages", 60))
    template = config.get("url_template", "")

    for page_number in range(start_page, start_page + max_pages):
        if len(results) >= MAX_ARTICLES_PER_SOURCE:
            break

        page_url = template.format(page=page_number)

        try:
            resp = await http_get(page_url, "text/html,application/xhtml+xml")
            if not resp.ok:
                print("MOC pagination HTTP", page_number, resp.status)
                continue

            html = await response_text(resp)
            pages_visited += 1
            raw_links_seen += len(re.findall(r'chitiettin\\.aspx|/vn/tin-tuc/', html, flags=re.I))
            links = discover_moc_article_links(html, page_url)

            for url, label in links:
                key = canonicalize_url(url)
                if not key or key in seen:
                    continue
                seen.add(key)

                try:
                    item = await fetch_html_candidate(url, source)
                    if not item.get("title"):
                        item["title"] = truncate(label, MAX_TITLE_CHARS)
                    if item.get("title"):
                        results.append(item)
                except Exception as exc:
                    print("MOC pagination article failed:", url, str(exc))

                if len(results) >= MAX_ARTICLES_PER_SOURCE:
                    break

        except Exception as exc:
            print("MOC pagination page error:", page_number, str(exc))

    source["_pagination_found"] = len(results)
    source["_pagination_pages_visited"] = pages_visited
    source["_pagination_raw_links_seen"] = raw_links_seen
    return results[:MAX_ARTICLES_PER_SOURCE]


async def crawl_one_source(env, source):
    source_id = await get_or_create_source(env, source)
    started = now_iso()
    found = 0
    new_count = 0
    duplicate_count = 0
    skipped_count = 0
    images_stored = 0
    images_failed = 0
    status = "success"
    error_message = None

    try:
        items = []

        if source.get("feed_urls"):
            items.extend(await crawl_multi_feed_source(source))

        if source.get("name") == "Ministry of Construction" or source.get("pagination", {}).get("enabled"):
            pagination_items = await crawl_pagination_source(source)
            seen_urls = {canonicalize_url(x.get("url", "")) for x in items}
            for item in pagination_items:
                key = canonicalize_url(item.get("url", ""))
                if key and key not in seen_urls:
                    seen_urls.add(key)
                    items.append(item)

        if not source.get("feed_urls") and source.get("feed_url"):
            items = await crawl_rss_source(source)

        if not source.get("feed_urls") and not source.get("feed_url") and not source.get("pagination", {}).get("enabled"):
            items = await crawl_html_source(source)

        def _item_ts(value):
            try:
                return datetime.fromisoformat((value or "").replace("Z", "+00:00")).timestamp()
            except Exception:
                return 0

        items.sort(key=lambda x: _item_ts(x.get("published_at")), reverse=True)
        items = items[:MAX_ARTICLES_PER_SOURCE]

        found = len(items)
        for item in items:
            try:
                result = await process_item(env, source, source_id, item)
                if result["status"] == "new":
                    new_count += 1
                    if result.get("stored_image_url"):
                        images_stored += 1
                    elif result.get("image_error"):
                        images_failed += 1
                elif result["status"] == "duplicate":
                    duplicate_count += 1
                elif result["status"] == "skip":
                    skipped_count += 1
            except Exception as exc:
                print("item processing error:", source["name"], str(exc))

        await update_source_last_crawled(env, source_id)
    except Exception as exc:
        status = "error"
        error_message = str(exc)
        print("source error:", source["name"], error_message)

    finished = now_iso()
    await insert_log(env, {
        "source_id": source_id,
        "started_at": started,
        "finished_at": finished,
        "status": status,
        "articles_found": found,
        "articles_new": new_count,
        "error_message": error_message,
    })

    return {
        "source": source["name"],
        "status": status,
        "found": found,
        "new": new_count,
        "duplicates": duplicate_count,
        "skipped": skipped_count,
        "images_stored": images_stored,
        "images_failed": images_failed,
        "pagination_found": source.get("_pagination_found", 0),
        "pagination_pages_visited": source.get("_pagination_pages_visited", 0),
        "pagination_raw_links_seen": source.get("_pagination_raw_links_seen", 0),
        "error": error_message,
    }


async def run_collector(env):
    results = []
    for source in SOURCES:
        if not source.get("active", True):
            continue
        try:
            result = await crawl_one_source(env, source)
        except Exception as exc:
            result = {
                "source": source["name"],
                "status": "error",
                "found": 0,
                "new": 0,
                "duplicates": 0,
                "skipped": 0,
                "images_stored": 0,
                "images_failed": 0,
                "error": str(exc),
            }
        results.append(result)

    totals = {
        "sources": len(results),
        "success": sum(1 for r in results if r["status"] == "success"),
        "errors": sum(1 for r in results if r["status"] == "error"),
        "found": sum(r["found"] for r in results),
        "new": sum(r["new"] for r in results),
        "duplicates": sum(r["duplicates"] for r in results),
        "skipped": sum(r.get("skipped", 0) for r in results),
        "images_stored": sum(r.get("images_stored", 0) for r in results),
        "images_failed": sum(r.get("images_failed", 0) for r in results),
    }

    print("COLLECTOR TOTALS:", json.dumps(totals))
    return {"time": now_iso(), "totals": totals, "sources": results}


# ============================================================
# CLOUDFLARE WORKER ENTRYPOINT
# ============================================================


class Default(WorkerEntrypoint):

    async def fetch(self, request):
        try:
            url = request.url
            if not getattr(self, "env", None):
                env = self.env
            else:
                env = self.env

            # Simple health endpoint. Any supported action is handled below.
            # Unknown/no action returns health.
            if not any(("action=" + a) in url for a in ("run", "discover", "process", "inspect")):
                payload = {
                    "service": "Turner Vietnam News Collector",
                    "version": BUILD_VERSION,
                    "status": "ok",
                    "time": now_iso(),
                    "sources_configured": len([s for s in SOURCES if s.get("active", True)]),
                    "moc_pagination_enabled": any(
                        s.get("name") == "Ministry of Construction" for s in SOURCES
                    ),
                    "moc_pagination_config": next(
                        (s.get("pagination", {}) for s in SOURCES
                         if s.get("name") == "Ministry of Construction"),
                        {}
                    ),
                    "cron": CRON_SCHEDULE,
                    "message": "Collector is deployed. Scheduled runs are handled by Cloudflare Cron.",
                }
                return Response(json.dumps(payload, indent=2), headers={"Content-Type": "application/json"})

            # Protected manual run.
            token = ""
            try:
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(url).query)
                token = (q.get("token") or [""])[0]
            except Exception:
                token = ""

            if not env.COLLECTOR_TOKEN or token != env.COLLECTOR_TOKEN:
                return Response("Unauthorized", status=401)

            q = parse_qs(urlparse(url).query)
            requested_source = (q.get("source") or [""])[0].strip()
            action = (q.get("action") or ["run"])[0].strip().lower()

            if action == "inspect":
                inspect_url = (q.get("url") or [""])[0].strip()
                if not inspect_url:
                    return Response(
                        json.dumps({"status": "error", "error": "Missing url"}),
                        status=400,
                        headers={"Content-Type": "application/json"},
                    )

                resp = await http_get(inspect_url, "text/html,application/xhtml+xml")
                if not resp.ok:
                    return Response(
                        json.dumps({
                            "status": "error",
                            "error": f"HTTP {resp.status}",
                            "url": inspect_url
                        }, indent=2),
                        status=502,
                        headers={"Content-Type": "application/json"},
                    )

                html = await response_text(resp)
                title = extract_title(html)
                published = extract_moc_published(html) if "moc.gov.vn" in inspect_url.lower() else extract_published(html)
                image = extract_moc_image(html, inspect_url) if "moc.gov.vn" in inspect_url.lower() else extract_image(html, inspect_url)

                interesting = []
                for m in re.finditer(
                    r'<([a-z0-9]+)[^>]*(?:class|id)=["\'][^"\']*(?:article|content|detail|news|post|date|time|publish|main)[^"\']*["\'][^>]*>',
                    html,
                    flags=re.I,
                ):
                    interesting.append(m.group(0)[:500])
                    if len(interesting) >= 50:
                        break

                body = find_first([r'<body[^>]*>([\s\S]*?)</body>'], html) or html
                text_preview = clean_text(body)[:5000]

                time_marker = "News_Time_Post"
                time_pos = html.find(time_marker)
                if time_pos >= 0:
                    time_start = max(0, time_pos - 1000)
                    time_end = min(len(html), time_pos + 3000)
                    news_time_html = html[time_start:time_end]
                    news_time_context = clean_text(unescape(news_time_html))
                else:
                    news_time_html = ""
                    news_time_context = ""

                return Response(
                    json.dumps({
                        "time": now_iso(),
                        "version": BUILD_VERSION,
                        "action": "inspect",
                        "status": "ok",
                        "url": inspect_url,
                        "html_length": len(html),
                        "title": title,
                        "published": published,
                        "image": image,
                        "interesting_tags": interesting,
                        "news_time_html": news_time_html,
                        "news_time_context": news_time_context,
                        "text_preview": text_preview,
                    }, indent=2),
                    headers={"Content-Type": "application/json"},
                )

            if action in {"discover", "process"}:
                source_name = requested_source or "Ministry of Construction"
                matches = [s for s in SOURCES if s.get("name", "").lower() == source_name.lower()]
                if not matches:
                    return Response(
                        json.dumps({"status": "error", "error": "Unknown source", "source": source_name}),
                        status=404,
                        headers={"Content-Type": "application/json"},
                    )
                source = matches[0]

                if action == "discover":
                    start_page = int((q.get("page") or ["1"])[0])
                    pages = min(int((q.get("pages") or [str(DISCOVERY_PAGES_PER_RUN)])[0]), DISCOVERY_PAGES_PER_RUN)
                    result = await discover_moc_batch(env, source, start_page, pages)
                else:
                    limit = min(int((q.get("limit") or [str(PROCESS_BATCH_SIZE)])[0]), PROCESS_BATCH_SIZE)
                    result = await process_queue_batch(env, source, limit)

                return Response(
                    json.dumps({
                        "time": now_iso(),
                        "version": BUILD_VERSION,
                        "action": action,
                        "result": result,
                    }, indent=2),
                    headers={"Content-Type": "application/json"},
                )

            if requested_source:
                matches = [s for s in SOURCES if s.get("name", "").lower() == requested_source.lower()]
                if not matches:
                    return Response(
                        json.dumps({"status": "error", "error": "Unknown source", "source": requested_source}),
                        status=404,
                        headers={"Content-Type": "application/json"},
                    )
                source = matches[0]
                result = await crawl_one_source(env, source)
                return Response(
                    json.dumps({"time": now_iso(), "version": BUILD_VERSION, "totals": {
                        "sources": 1,
                        "success": 1 if result["status"] == "success" else 0,
                        "errors": 1 if result["status"] == "error" else 0,
                        "found": result["found"],
                        "new": result["new"],
                        "duplicates": result["duplicates"],
                        "skipped": result.get("skipped", 0),
                        "images_stored": result.get("images_stored", 0),
                        "images_failed": result.get("images_failed", 0),
                        "pagination_found": result.get("pagination_found", 0),
                        "pagination_pages_visited": result.get("pagination_pages_visited", 0),
                        "pagination_raw_links_seen": result.get("pagination_raw_links_seen", 0),
                        "pagination_config_enabled": bool(
                            result.get("pagination_found", 0)
                            or result.get("pagination_pages_visited", 0)
                        ),
                    }, "sources": [result]}, indent=2),
                    headers={"Content-Type": "application/json"},
                )

            result = await run_collector(env)
            return Response(json.dumps(result, indent=2), headers={"Content-Type": "application/json"})

        except Exception as exc:
            return Response(
                json.dumps({"status": "error", "error": str(exc)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def scheduled(self, controller, env, ctx):
        # Safe scheduled batch: discover a few MOC pages, then process a few queued articles.
        moc = next((s for s in SOURCES if s.get("name") == "Ministry of Construction"), None)
        if moc:
            await discover_moc_batch(env, moc, 1, DISCOVERY_PAGES_PER_RUN)
            await process_queue_batch(env, moc, PROCESS_BATCH_SIZE)
