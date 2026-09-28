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
MAX_ARTICLES_PER_SOURCE = 20
MAX_CONTENT_CHARS = 120000
MAX_DESCRIPTION_CHARS = 1000
MAX_TITLE_CHARS = 500
MAX_IMAGE_BYTES = 8 * 1024 * 1024

# Cloudflare Cron schedules are UTC.
# Example: 0 * * * * = every hour on the hour.
CRON_SCHEDULE = "0 * * * *"

# Add/edit sources here. For the most reliable ingestion, fill feed_url
# with an official RSS/Atom feed. When feed_url is empty, the collector
# performs conservative HTML discovery from page_url.
SOURCES = [
    # ---- Vietnam / official ----
    {
        "name": "Ministry of Construction",
        "page_url": "https://moc.gov.vn/",
        "feed_url": "",
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
        "active": False,
    },
    {
        "name": "Vietnam National Statistics Office",
        "page_url": "https://www.nso.gov.vn/",
        "feed_url": "",
        "category": "Vietnam Market",
        "content_type": "MARKET_INTELLIGENCE",
        "active": False,
    },

    # ---- Construction / technology ----
    {
        "name": "ENR",
        "page_url": "https://www.enr.com/",
        "feed_url": "",
        "category": "Construction",
        "content_type": "NEWS",
        "active": False,
    },
    {
        "name": "Construction Dive",
        "page_url": "https://www.constructiondive.com/",
        "feed_url": "",
        "category": "Construction",
        "content_type": "NEWS",
        "active": False,
    },
    {
        "name": "Procore",
        "page_url": "https://www.procore.com/blog",
        "feed_url": "",
        "category": "Construction Technology",
        "content_type": "UPDATE",
        "active": False,
    },
    {
        "name": "OpenSpace",
        "page_url": "https://www.openspace.ai/blog/",
        "feed_url": "",
        "category": "Construction Technology",
        "content_type": "UPDATE",
        "active": False,
    },
    {
        "name": "DroneDeploy",
        "page_url": "https://www.dronedeploy.com/blog",
        "feed_url": "",
        "category": "Construction Technology",
        "content_type": "UPDATE",
        "active": False,
    },
    {
        "name": "HoloBuilder / FARO",
        "page_url": "https://www.holobuilder.com/",
        "feed_url": "",
        "category": "Construction Technology",
        "content_type": "UPDATE",
        "active": False,
    },
    {
        "name": "Autodesk Construction",
        "page_url": "https://www.autodesk.com/blogs/construction/",
        "feed_url": "",
        "category": "Construction Technology",
        "content_type": "NEWS",
        "active": False,
    },

    # ---- Microsoft 365 ----
    {
        "name": "Microsoft SharePoint",
        "page_url": "https://techcommunity.microsoft.com/category/SharePoint/blog/SharePoint",
        "feed_url": "",
        "category": "Microsoft 365",
        "content_type": "NEWS",
        "active": False,
    },
    {
        "name": "Microsoft Teams",
        "page_url": "https://techcommunity.microsoft.com/category/MicrosoftTeams/blog/MicrosoftTeamsBlog",
        "feed_url": "",
        "category": "Microsoft 365",
        "content_type": "NEWS",
        "active": False,
    },
    {
        "name": "Microsoft Planner",
        "page_url": "https://techcommunity.microsoft.com/t5/planner-blog/bg-p/PlannerBlog",
        "feed_url": "",
        "category": "Microsoft 365",
        "content_type": "NEWS",
        "active": False,
    },
    {
        "name": "Microsoft Power BI",
        "page_url": "https://powerbi.microsoft.com/blog/",
        "feed_url": "",
        "category": "Microsoft 365",
        "content_type": "NEWS",
        "active": False,
    },
    {
        "name": "Microsoft Power Platform",
        "page_url": "https://www.microsoft.com/en-us/power-platform/blog/",
        "feed_url": "",
        "category": "Microsoft 365",
        "content_type": "NEWS",
        "active": False,
    },

    # ---- Turner ----
    {
        "name": "Turner Insights",
        "page_url": "https://www.turnerconstruction.com/insights",
        "feed_url": "",
        "category": "Turner Global",
        "content_type": "TURNER_INTERNAL",
        "active": False,
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
        "/rest/v1/hub_hub_article_images",
        image_record,
        {"Prefer": "return=minimal"},
    )
    if not resp.ok:
        raise RuntimeError("Supabase image record insert failed: " + await resp.text())


async def insert_log(env, log_record):
    try:
        await sb_request(env, "POST", "/rest/v1/hub_hub_crawl_logs", log_record, {"Prefer": "return=minimal"})
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
    value = html_meta(html, ["og:image", "twitter:image"])
    if value:
        return urljoin(base_url, value)
    m = re.search(r'<img[^>]+(?:src|data-src)=["\']([^"\']+)["\']', html, flags=re.I)
    if m:
        return urljoin(base_url, m.group(1))
    return ""


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


def discover_html_links(html, base_url):
    links = []
    seen = set()
    for m in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([\s\S]*?)</a>', html, flags=re.I):
        href = urljoin(base_url, m.group(1))
        label = clean_text(m.group(2))
        if not href.startswith(("http://", "https://")):
            continue
        href = canonicalize_url(href)
        if href in seen:
            continue
        seen.add(href)
        # Skip obvious non-article assets / utility pages.
        path = urlparse(href).path.lower()
        if any(path.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".svg", ".pdf", ".zip")):
            continue
        if any(x in href.lower() for x in ("/login", "/signup", "/privacy", "/terms", "/search", "/contact")):
            continue
        if len(label) < 15:
            continue
        links.append((href, label))
        if len(links) >= MAX_ARTICLES_PER_SOURCE:
            break
    return links


async def fetch_html_candidate(url, source):
    resp = await http_get(url, "text/html,application/xhtml+xml")
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status}")
    html = await response_text(resp)
    return {
        "title": extract_title(html),
        "url": canonicalize_url(url),
        "description": extract_description(html),
        "author": extract_author(html),
        "published_at": extract_published(html) or now_iso(),
        "image_url": extract_image(html, url),
        "content": extract_main_text(html),
        "category": source.get("category", "General"),
        "content_type": source.get("content_type", "NEWS"),
    }


async def crawl_html_source(source):
    results = []
    resp = await http_get(source["page_url"], "text/html,application/xhtml+xml")
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status}")
    html = await response_text(resp)
    links = discover_html_links(html, source["page_url"])

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
    return enriched


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
        return ""
    try:
        image_url = canonicalize_url(image_url)
        resp = await http_get(image_url, "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8")
        if not resp.ok:
            return ""
        content_type = resp.headers.get("content-type", "application/octet-stream")
        if not content_type.lower().startswith("image/"):
            return ""

        data = await resp.arrayBuffer()
        # JS ArrayBuffer is exposed to Python as a bytes-like value in Workers.
        try:
            content = bytes(data)
        except Exception:
            content = data

        if len(content) > MAX_IMAGE_BYTES:
            return ""

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
        return public_url
    except Exception as exc:
        print("image store failed:", image_url, str(exc))
        return ""


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
        "published_at": item.get("published_at") or now_iso(),
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

    if article_id and record.get("image_url"):
        stored_image_url = await store_primary_image(
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

    return {"status": "new", "article_id": article_id, "stored_image_url": stored_image_url}


# ============================================================
# SOURCE CRAWLING
# ============================================================


async def crawl_one_source(env, source):
    source_id = await get_or_create_source(env, source)
    started = now_iso()
    found = 0
    new_count = 0
    duplicate_count = 0
    status = "success"
    error_message = None

    try:
        if source.get("feed_url"):
            items = await crawl_rss_source(source)
        else:
            items = await crawl_html_source(source)

        found = len(items)
        for item in items:
            try:
                result = await process_item(env, source, source_id, item)
                if result["status"] == "new":
                    new_count += 1
                elif result["status"] == "duplicate":
                    duplicate_count += 1
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

            # Simple health endpoint.
            if "action=run" not in url:
                payload = {
                    "service": "Turner Vietnam News Collector",
                    "status": "ok",
                    "time": now_iso(),
                    "sources_configured": len([s for s in SOURCES if s.get("active", True)]),
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

            result = await run_collector(env)
            return Response(json.dumps(result, indent=2), headers={"Content-Type": "application/json"})

        except Exception as exc:
            return Response(
                json.dumps({"status": "error", "error": str(exc)}),
                status=500,
                headers={"Content-Type": "application/json"},
            )

    async def scheduled(self, controller, env, ctx):
        # Cloudflare Cron Triggers invoke this method.
        await run_collector(env)
