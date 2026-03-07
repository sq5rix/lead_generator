#!/usr/bin/env python3
"""
Lead Scraper v3 – Hardened version with relevance filtering.
Uses SearXNG + Ollama/Llama (100% local, open-source).
"""

import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup

# ============================================================
# CONFIG
# ============================================================
SEARXNG_URL = os.getenv("SEARXNG_URL", "http://localhost:8888/search")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")

REQUEST_TIMEOUT = 15
SCRAPE_TIMEOUT = 15
MAX_CONTENT_LENGTH = 8000
DELAY_BETWEEN_REQUESTS = 2
MAX_CONTACT_PAGES = 3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

SKIP_DOMAINS = [
    "youtube.com", "facebook.com", "yelp.com", "yellowpages.com",
    "linkedin.com", "wikipedia.org", "amazon.com", "ebay.com",
    "reddit.com", "twitter.com", "instagram.com", "tiktok.com",
    "pinterest.com", "tripadvisor.com", "mapquest.com", "apple.com",
    "google.com", "googleapis.com", "bbb.org", "crunchbase.com",
    "zoominfo.com", "dnb.com", "hoovers.com", "manta.com",
    "superpages.com", "whitepages.com", "chamberofcommerce.com",
    "barnesandnoble.com", "bookshop.org", "thriftbooks.com",
    "abebooks.com", "powells.com", "betterworldbooks.com",
    "penguinrandomhouse.com", "harpercollins.com", "macmillan.com",
    "simonandschuster.com", "hachette.com", "randomhouse.com",
    "listsxpanders.com", "educationdatalists.com", "libro.fm",
    "infobel.com", "bookstores.com", "greatbooks.org",
    "oxfordonlinepractice.com", "oxfordlearnersdictionaries.com",
    "graywolfpress.org", "candlewick.com", "chroniclebooks.com",
    "ucpress.edu", "mpsvirginia.com", "milkweed.org",
    "disney.com", "independent.com",
]

JUNK_EMAIL_DOMAINS = [
    "example.com", "test.com", "sentry.io", "wixpress.com",
    "w3.org", "schema.org", "googleapis.com", "cloudflare.com",
    "wordpress.com", "gravatar.com", "yoursite.com", "email.com",
    "domain.com", "website.com", "company.com", "squarespace.com",
    "shopify.com", "weebly.com", "wix.com", "godaddy.com",
]

JUNK_PHRASES = [
    "mailing list provider", "email list broker", "buy leads",
    "data provider", "b2b database", "marketing list",
    "lead generation service", "bulk email service",
    "sales leads provider", "business database provider",
]

REGION_MAP = {
    "us": "us", "uk": "gb", "eu": "eu",
    "canada": "ca", "au": "au", "all": None,
}

LOCATION_NAMES = {
    "us": "United States", "gb": "United Kingdom",
    "eu": "Europe", "ca": "Canada", "au": "Australia",
    None: "",
}


# ============================================================
# DIAGNOSTICS - run first to find what's broken
# ============================================================
def run_diagnostics(business_type, location, region):
    """Test each component individually to find the bottleneck."""
    print("\n" + "=" * 60)
    print("  RUNNING DIAGNOSTICS")
    print("=" * 60)

    # Test 1: SearXNG basic
    print("\n[TEST 1] SearXNG basic search...")
    try:
        params = {"q": f"{business_type} {location}", "format": "json"}
        r = requests.get(SEARXNG_URL, params=params, timeout=10)
        data = r.json()
        results = data.get("results", [])
        print(f"  Raw results: {len(results)}")
        if results:
            for res in results[:3]:
                print(f"    - {res.get('title', 'N/A')[:60]}")
                print(f"      {res.get('url', 'N/A')[:70]}")
        else:
            print("  ⚠ NO RESULTS - SearXNG may have no working engines!")
            print("  Try: curl 'http://localhost:8888/search?q=test&format=json' | python3 -m json.tool | head -30")
            return False
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        return False

    # Test 2: SearXNG with our params
    print("\n[TEST 2] SearXNG with engine params...")
    try:
        params = {
            "q": f"{business_type} {location} contact email",
            "format": "json",
            "categories": "general",
            "language": "en",
        }
        if region:
            params["language"] = f"en-{region.upper()}"
            params["region"] = region
        r = requests.get(SEARXNG_URL, params=params, timeout=10)
        data = r.json()
        results = data.get("results", [])
        print(f"  Results with params: {len(results)}")
        if not results:
            print("  ⚠ Region/language params may be killing results")
            print("  Trying without region...")
            params2 = {"q": f"{business_type} {location} contact email", "format": "json"}
            r2 = requests.get(SEARXNG_URL, params=params2, timeout=10)
            results2 = r2.json().get("results", [])
            print(f"  Results without region: {len(results2)}")
            if results2:
                print("  → Region params are the problem! Will disable them.")
                return "no_region"
    except Exception as e:
        print(f"  ✗ FAILED: {e}")

    # Test 3: After skip domain filtering
    print("\n[TEST 3] Domain filtering...")
    if results:
        kept = 0
        skipped = 0
        for res in results:
            url = res.get("url", "")
            if _should_skip_url(url):
                skipped += 1
            else:
                kept += 1
        print(f"  Kept: {kept}, Skipped: {skipped}")
        if kept == 0:
            print("  ⚠ ALL results filtered out by domain skip list!")

    # Test 4: Relevance filtering
    print("\n[TEST 4] Relevance keyword check...")
    if results:
        passed = 0
        for res in results:
            if check_relevance_fast(res.get("title", ""), res.get("content", ""), res.get("url", ""), business_type):
                passed += 1
        print(f"  Passed keyword check: {passed}/{len(results)}")
        if passed == 0:
            print(f"  ⚠ ALL filtered out! Business type '{business_type}' not in any title/snippet")
            print(f"  Try a simpler term (e.g., 'bookstore' instead of 'independent bookstores')")

    # Test 5: Ollama
    print("\n[TEST 5] Ollama connection...")
    try:
        r = requests.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": "Say OK", "stream": False,
                  "options": {"num_predict": 5}},
            timeout=30,
        )
        r.raise_for_status()
        resp = r.json().get("response", "")
        print(f"  Ollama says: {resp.strip()[:50]}")
    except Exception as e:
        print(f"  ✗ Ollama FAILED: {e}")

    print("\n" + "=" * 60)
    print("  DIAGNOSTICS COMPLETE")
    print("=" * 60)
    return True


# ============================================================
# LLAMA QUERY GENERATION
# ============================================================
def llama_build_queries(business_type, location, num_queries=7):
    """Use Llama to generate search queries."""
    prompt = f"""Generate exactly {num_queries} search engine queries to find independent local {business_type} in {location}.

CRITICAL REQUIREMENTS:
- Every query MUST contain the exact word "{business_type}" (or a very close synonym)
- Every query must target finding the STORE'S OWN WEBSITE (not directories, not aggregators)
- Queries should help find pages that contain an email address
- Mix these strategies across the {num_queries} queries:
  * Some with "contact" or "contact us" or "about us"
  * Some with "@gmail.com" OR "@yahoo.com" OR "info@" OR "contact@"
  * Some targeting specific cities or states within {location}
  * Some with "independent" or "indie" or "local" or "neighborhood"
- Do NOT include any explanation, just the queries

EXAMPLES of good queries for "bookstores" in "United States":
independent bookstore Portland Oregon "contact us" email
"indie bookshop" California "@gmail.com" OR "info@"
local used bookstore Texas "about us" phone email

Now generate {num_queries} queries for "{business_type}" in "{location}".
Output ONLY the queries, one per line, nothing else."""

    try:
        print("  [Llama] Generating search queries...")
        r = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.7, "num_predict": 1500},
            },
            timeout=90,
        )
        r.raise_for_status()
        response_text = r.json().get("response", "")

        queries = _parse_query_lines(response_text, business_type)

        if len(queries) >= 3:
            print(f"  [Llama] Generated {len(queries)} queries:")
            for i, q in enumerate(queries, 1):
                print(f"    {i}. {q[:90]}")
            return queries[:num_queries]
        else:
            print(f"  [Llama] Only got {len(queries)} usable queries, supplementing with fallback")
            extras = fallback_queries(business_type, location)
            combined = queries + [q for q in extras if q not in queries]
            return combined[:num_queries]

    except Exception as e:
        print(f"  [Llama] Failed: {e}")
        print("  [Llama] Using fallback queries")
        return fallback_queries(business_type, location)


def _parse_query_lines(text, business_type):
    """Parse Llama output into clean query lines, validate each."""
    raw_lines = text.strip().split("\n")
    queries = []
    bt_lower = business_type.lower()
    bt_words = set(bt_lower.split())

    for line in raw_lines:
        line = line.strip()
        line = re.sub(r'^[\d]+[.):\-]\s*', '', line)
        line = re.sub(r'^[-*•]\s*', '', line)
        line = re.sub(r'^`+|`+$', '', line)
        line = line.strip()

        if len(line) < 15:
            continue

        skip_phrases = [
            "here are", "these queries", "note:", "explanation",
            "this query", "sure,", "certainly", "of course",
            "below are", "i'll generate", "generated",
        ]
        if any(p in line.lower() for p in skip_phrases):
            continue

        line_lower = line.lower()
        has_bt = bt_lower in line_lower
        if not has_bt:
            has_bt = any(w in line_lower for w in bt_words if len(w) > 3)
        if not has_bt:
            # Also check singular/plural
            if bt_lower.endswith("s"):
                has_bt = bt_lower[:-1] in line_lower
            else:
                has_bt = (bt_lower + "s") in line_lower
        if not has_bt:
            print(f"    [Llama] Rejected (missing '{business_type}'): {line[:60]}")
            continue

        queries.append(line)

    return queries


def fallback_queries(business_type, location):
    """Reliable fallback queries - simpler, no negative operators."""
    bt = business_type
    loc = location
    return [
        f'{bt} {loc} "contact us" email',
        f'{bt} {loc} "@gmail.com" OR "@yahoo.com"',
        f'local {bt} {loc} email phone',
        f'independent {bt} {loc} "info@" OR "contact@"',
        f'{bt} {loc} "about us" email address',
        f'indie {bt} shop {loc} contact',
        f'{bt} store {loc} email website',
    ]


# ============================================================
# SEARXNG SEARCH
# ============================================================
def search_searxng(query, region=None, max_results=20, use_region=True):
    """Search SearXNG, return list of {url, title, snippet}."""
    results = []
    seen_urls = set()
    pages_to_try = min(3, max(1, max_results // 10))

    for page in range(1, pages_to_try + 1):
        params = {
            "q": query,
            "format": "json",
            "pageno": page,
        }
        # Only add region params if they work (diagnostics may disable)
        if use_region and region:
            params["language"] = f"en-{region.upper()}"

        try:
            r = requests.get(SEARXNG_URL, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"    [!] SearXNG error page {page}: {e}")
            break

        page_results = data.get("results", [])
        if not page_results:
            break

        for item in page_results:
            url = item.get("url", "")
            if not url or url in seen_urls:
                continue
            if _should_skip_url(url):
                continue

            title = item.get("title", "")
            snippet = item.get("content", "")

            combined = (title + " " + snippet).lower()
            if any(phrase in combined for phrase in JUNK_PHRASES):
                continue

            seen_urls.add(url)
            results.append({"url": url, "title": title, "snippet": snippet})

        time.sleep(DELAY_BETWEEN_REQUESTS)

    return results[:max_results]


def _should_skip_url(url):
    """Skip known junk domains."""
    url_lower = url.lower()
    for domain in SKIP_DOMAINS:
        if domain in url_lower:
            return True
    if any(url_lower.endswith(ext) for ext in [".pdf", ".doc", ".xls", ".zip", ".png", ".jpg"]):
        return True
    return False


# ============================================================
# RELEVANCE CHECKING
# ============================================================
def check_relevance_with_llama(title, snippet, url, business_type):
    """Quick Llama check: is this URL likely a real local business of the right type?"""
    prompt = f"""Is this a website for an actual local {business_type}?

Title: {title}
URL: {url}
Snippet: {snippet}

Answer ONLY "yes" or "no". Nothing else.
- "yes" = this is a real individual {business_type} (independent store/shop with its own website)
- "no" = this is a directory, publisher, aggregator, news article, unrelated business, or chain"""

    try:
        r = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.0, "num_predict": 10},
            },
            timeout=20,
        )
        r.raise_for_status()
        answer = r.json().get("response", "").strip().lower()
        return answer.startswith("yes")
    except Exception:
        return True


def check_relevance_fast(title, snippet, url, business_type):
    """Fast keyword-based relevance check."""
    combined = (title + " " + snippet + " " + url).lower()
    bt_lower = business_type.lower()
    bt_words = [w for w in bt_lower.split() if len(w) > 3]

    has_match = bt_lower in combined
    if not has_match:
        has_match = any(w in combined for w in bt_words)
    if not has_match:
        if bt_lower.endswith("s"):
            has_match = bt_lower[:-1] in combined
        else:
            has_match = (bt_lower + "s") in combined
    # Also check common synonyms
    if not has_match:
        synonym_map = {
            "bookstore": ["book shop", "bookshop", "book store", "books"],
            "coffee shop": ["cafe", "café", "coffee house", "coffeehouse", "roaster"],
            "restaurant": ["dining", "eatery", "bistro", "grill", "kitchen"],
            "bakery": ["bake shop", "bakehouse", "pastry", "patisserie"],
            "florist": ["flower shop", "floral", "flowers"],
        }
        for key, synonyms in synonym_map.items():
            if key in bt_lower or bt_lower in key:
                has_match = any(s in combined for s in synonyms)
                if has_match:
                    break

    return has_match


# ============================================================
# WEB SCRAPING
# ============================================================
def scrape_page(url):
    """Scrape a single page, return (cleaned_text, soup)."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=SCRAPE_TIMEOUT, allow_redirects=True)
        r.raise_for_status()

        content_type = r.headers.get("content-type", "")
        if "html" not in content_type.lower() and "text" not in content_type.lower():
            return "", None

        soup = BeautifulSoup(r.text, "html.parser")

        for tag in soup(["script", "style", "noscript", "svg", "img", "iframe"]):
            tag.decompose()

        text = soup.get_text(separator="\n", strip=True)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        return text[:MAX_CONTENT_LENGTH], soup
    except Exception:
        return "", None


def find_contact_links(soup, base_url):
    """Find actual contact/about page links from the page's navigation."""
    if not soup:
        return []

    contact_keywords = [
        "contact", "about", "reach", "connect", "get-in-touch",
        "find-us", "visit", "location", "info",
    ]

    links = []
    seen = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        link_text = a_tag.get_text(strip=True).lower()

        is_contact = False
        href_lower = href.lower()
        for kw in contact_keywords:
            if kw in href_lower or kw in link_text:
                is_contact = True
                break

        if not is_contact:
            continue

        full_url = urljoin(base_url, href)

        try:
            if urlparse(full_url).netloc != urlparse(base_url).netloc:
                continue
        except Exception:
            continue

        if full_url not in seen:
            seen.add(full_url)
            links.append(full_url)

    return links[:5]


def scrape_with_contact_pages(base_url):
    """Scrape main page + contact pages."""
    all_text = []

    main_text, soup = scrape_page(base_url)
    if main_text and len(main_text) > 50:
        all_text.append(f"--- MAIN: {base_url} ---\n{main_text}")

    contact_urls = find_contact_links(soup, base_url) if soup else []

    if not contact_urls:
        parsed = urlparse(base_url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
        contact_urls = [
            f"{domain}/contact",
            f"{domain}/contact-us",
            f"{domain}/about",
        ]

    scraped_count = 1
    for url in contact_urls:
        if scraped_count >= MAX_CONTACT_PAGES:
            break
        if url == base_url:
            continue

        time.sleep(0.5)
        text, _ = scrape_page(url)
        if text and len(text) > 50:
            all_text.append(f"--- PAGE: {url} ---\n{text}")
            scraped_count += 1

    combined = "\n\n".join(all_text)
    return combined[:MAX_CONTENT_LENGTH * 2]


# ============================================================
# EMAIL EXTRACTION
# ============================================================
def extract_emails_from_text(text):
    """Regex extract emails, filter junk."""
    if not text:
        return []
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(pattern, text)

    cleaned = []
    for e in emails:
        e_lower = e.lower()
        if any(e_lower.endswith(ext) for ext in [".png", ".jpg", ".gif", ".svg", ".css", ".js"]):
            continue
        domain = e_lower.split("@")[1]
        if domain in JUNK_EMAIL_DOMAINS:
            continue
        if e_lower not in cleaned:
            cleaned.append(e_lower)
    return cleaned


def is_valid_email(email):
    """Validate email format and domain."""
    if not email or email == "not found" or len(email) < 5:
        return False
    email = email.strip().lower()
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False
    domain = email.split('@')[1]
    if domain in JUNK_EMAIL_DOMAINS:
        return False
    return True


# ============================================================
# OLLAMA EXTRACTION
# ============================================================
def extract_with_ollama(page_text, business_type, source_url):
    """Extract leads using Ollama."""
    regex_emails = extract_emails_from_text(page_text)

    email_hint = ""
    if regex_emails:
        email_hint = f"\nEmails found on page: {', '.join(regex_emails[:10])}"

    prompt = f"""Extract business contact info from this webpage.

BUSINESS TYPE I NEED: {business_type}
Source URL: {source_url}
{email_hint}

STRICT RULES:
1. ONLY extract businesses that ARE actually {business_type}
2. If this page is NOT about a {business_type}, return: []
3. Email is REQUIRED - no email = skip that business
4. Do NOT invent or guess information
5. Return ONLY a JSON array, nothing else

JSON format:
[{{"name":"Store Name","email":"real@email.com","phone":"555-1234","address":"123 Main St, City, ST 12345","website":"storename.com","description":"short description"}}]

If this is NOT a {business_type} or has no email, return exactly: []

PAGE TEXT:
{page_text[:MAX_CONTENT_LENGTH]}"""

    try:
        r = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 2000},
            },
            timeout=120,
        )
        r.raise_for_status()
        response_text = r.json().get("response", "")
        leads = _parse_json_response(response_text)

        if not leads and regex_emails:
            if _text_seems_relevant(page_text, business_type):
                return _create_fallback_lead(regex_emails, source_url, page_text)

        return leads

    except Exception as e:
        print(f"    [!] Ollama error: {e}")
        if regex_emails and _text_seems_relevant(page_text, business_type):
            return _create_fallback_lead(regex_emails, source_url, page_text)
        return []


def _text_seems_relevant(text, business_type):
    """Check if page text mentions the business type."""
    text_lower = text.lower()
    bt_lower = business_type.lower()

    if bt_lower in text_lower:
        return True

    bt_words = [w for w in bt_lower.split() if len(w) > 3]
    if not bt_words:
        return bt_lower in text_lower
    matches = sum(1 for w in bt_words if w in text_lower)
    return matches >= max(1, len(bt_words) * 0.5)


def _parse_json_response(text):
    """Parse JSON array from Ollama response."""
    text = text.strip()
    start = text.find('[')
    end = text.rfind(']')

    if start == -1 or end == -1:
        return []

    json_str = text[start:end + 1]

    try:
        leads = json.loads(json_str)
        if isinstance(leads, list):
            return leads
    except json.JSONDecodeError:
        json_str = re.sub(r',\s*]', ']', json_str)
        json_str = re.sub(r',\s*}', '}', json_str)
        try:
            leads = json.loads(json_str)
            if isinstance(leads, list):
                return leads
        except Exception:
            pass

    return []


def _create_fallback_lead(emails, source_url, page_text=""):
    """Create a basic lead from regex emails when Ollama fails."""
    domain = _clean_website(source_url)
    domain_root = domain.split('.')[0] if domain and domain != "not found" else ""

    domain_emails = [e for e in emails if domain_root and domain_root in e]
    if not domain_emails:
        domain_emails = [e for e in emails if not any(
            generic in e for generic in ["noreply", "no-reply", "mailer-daemon", "postmaster"]
        )]
    if not domain_emails:
        domain_emails = emails[:1]

    email = domain_emails[0] if domain_emails else None
    if not email or not is_valid_email(email):
        return []

    name = domain_root.replace("-", " ").replace("_", " ").title() if domain_root else "Unknown"

    return [{
        "name": name,
        "email": email,
        "phone": "not found",
        "address": "not found",
        "website": domain,
        "description": "Auto-extracted (fallback)",
    }]


# ============================================================
# LEAD VALIDATION
# ============================================================
def _clean_website(url):
    """Extract clean domain."""
    if not url or url == "not found":
        return "not found"
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^www\.', '', url)
    url = url.split('/')[0].split('?')[0].split('#')[0]
    return url.strip().lower()


def validate_leads(leads, source_url=""):
    """Filter and clean leads. Email REQUIRED."""
    valid = []
    seen_emails = set()

    for lead in leads:
        if not isinstance(lead, dict):
            continue

        name = str(lead.get("name", "not found")).strip()
        email = str(lead.get("email", "not found")).strip().lower()
        phone = str(lead.get("phone", "not found")).strip()
        address = str(lead.get("address", "not found")).strip()
        website = str(lead.get("website", "not found")).strip()
        description = str(lead.get("description", "not found")).strip()

        if not is_valid_email(email):
            print(f"    Dropped (bad email): {name} | {email}")
            continue

        if email in seen_emails:
            print(f"    Dropped (duplicate): {email}")
            continue
        seen_emails.add(email)

        website = _clean_website(website)
        if website == "not found" and source_url:
            website = _clean_website(source_url)

        valid.append({
            "name": name,
            "email": email,
            "phone": phone,
            "address": address,
            "website": website,
            "description": description,
            "source_url": source_url,
        })

    return valid


# ============================================================
# CSV OUTPUT
# ============================================================
def save_to_csv(leads, business_type, region):
    """Save leads to CSV."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_type = re.sub(r'[^a-zA-Z0-9]', '_', business_type.lower())
    filename = f"leads_{safe_type}_{region}_{timestamp}.csv"

    fieldnames = ["name", "email", "phone", "address", "website", "description", "source_url"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead)
    return filename


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("  LEAD SCRAPER v3 (Hardened)")
    print("  SearXNG + Ollama/Llama – 100% local & open source")
    print("=" * 60)

    # ------- check services -------
    print("\nChecking services...")
    try:
        requests.get(SEARXNG_URL.replace("/search", "/"), timeout=5)
        print("  ✓ SearXNG OK")
    except Exception:
        print(f"  ✗ SearXNG not reachable at {SEARXNG_URL}")
        sys.exit(1)

    try:
        requests.get(OLLAMA_URL.replace("/api/generate", "/"), timeout=5)
        print("  ✓ Ollama OK")
    except Exception:
        print(f"  ✗ Ollama not reachable at {OLLAMA_URL}")
        sys.exit(1)

    # ------- user input -------
    print()
    business_type = input("Business type (e.g. 'bookstores', 'coffee shops'): ").strip()
    if not business_type:
        print("Need a business type!")
        sys.exit(1)

    print("Regions: us, uk, eu, canada, au, all")
    region_input = input("Region [us]: ").strip().lower() or "us"
    region = REGION_MAP.get(region_input, "us")
    location = LOCATION_NAMES.get(region, region_input)

    max_leads_str = input("Max leads [50]: ").strip()
    max_leads = int(max_leads_str) if max_leads_str.isdigit() else 50

    # ------- diagnostics first -------
    print("\nRun diagnostics first? (recommended) [y/n]: ", end="")
    run_diag = input().strip().lower()
    use_region = True

    if run_diag != "n":
        diag_result = run_diagnostics(business_type, location, region)
        if diag_result == "no_region":
            use_region = False
            print("\n  ⚠ Disabling region params for this run")
        elif diag_result is False:
            print("\n  Fix SearXNG first!")
            sys.exit(1)

    print("\nQuery generation:")
    print("  1. AI-powered (Llama) [default]")
    print("  2. Manual templates (faster, more reliable)")
    query_mode = input("Choose [1]: ").strip() or "1"

    use_relevance_llm = False
    print("Use Llama for relevance pre-check? (slower) [y/N]: ", end="")
    relevance_input = input().strip().lower()
    if relevance_input in ("y", "yes"):
        use_relevance_llm = True

    print(f"\n{'=' * 60}")
    print(f"  Business:    {business_type}")
    print(f"  Region:      {location or 'worldwide'}")
    print(f"  Max leads:   {max_leads}")
    print(f"  Query mode:  {'AI (Llama)' if query_mode == '1' else 'Manual'}")
    print(f"  Relevance:   {'Llama + keyword' if use_relevance_llm else 'Keyword only (fast)'}")
    print(f"  Region params: {'on' if use_region else 'OFF'}")
    print(f"{'=' * 60}")

    # ------- build queries -------
    print("\nStep 1: Building search queries...")
    if query_mode == "1":
        queries = llama_build_queries(business_type, location)
    else:
        queries = fallback_queries(business_type, location)

    # ------- search -------
    print("\nStep 2: Searching...")
    all_results = []
    seen_urls = set()

    for i, query in enumerate(queries, 1):
        print(f"\n  [{i}/{len(queries)}] {query[:85]}...")
        results = search_searxng(query, region, max_results=15, use_region=use_region)

        new_count = 0
        for r in results:
            if r["url"] not in seen_urls:
                seen_urls.add(r["url"])
                all_results.append(r)
                new_count += 1

        print(f"    +{new_count} new ({len(all_results)} total URLs)")

        # If first query got 0, try without region
        if i == 1 and new_count == 0 and use_region:
            print("    ⚠ Retrying without region params...")
            results = search_searxng(query, region, max_results=15, use_region=False)
            for r in results:
                if r["url"] not in seen_urls:
                    seen_urls.add(r["url"])
                    all_results.append(r)
                    new_count += 1
            if new_count > 0:
                use_region = False
                print(f"    → Disabled region params (+{new_count} results)")

        time.sleep(DELAY_BETWEEN_REQUESTS)

    if not all_results:
        print("\n  ✗ No search results at all!")
        print("  Possible fixes:")
        print("    1. Check SearXNG: curl 'http://localhost:8888/search?q=test&format=json'")
        print("    2. Try simpler business type")
        print("    3. Try region 'all'")
        sys.exit(1)

    # ------- relevance filter -------
    print(f"\nStep 3: Relevance filtering {len(all_results)} URLs...")
    relevant_results = []

    for r in all_results:
        title = r["title"]
        snippet = r["snippet"]
        url = r["url"]

        if not check_relevance_fast(title, snippet, url, business_type):
            print(f"  ✗ Skip: {title[:55]}")
            continue

        if use_relevance_llm:
            if not check_relevance_with_llama(title, snippet, url, business_type):
                print(f"  ✗ Llama: {title[:55]}")
                continue

        print(f"  ✓ Pass: {title[:55]}")
        relevant_results.append(r)

    print(f"\n  {len(relevant_results)}/{len(all_results)} passed relevance check")

    # If keyword filter killed everything, fall back to all results
    if not relevant_results and all_results:
        print("  ⚠ Keyword filter too strict, using ALL results instead")
        relevant_results = all_results

    if not relevant_results:
        print("\n  No results to process.")
        sys.exit(1)

    # ------- scrape & extract -------
    print(f"\nStep 4: Scraping & extracting leads from {len(relevant_results)} URLs...")
    all_leads = []
    seen_emails = set()

    for i, result in enumerate(relevant_results, 1):
        if len(all_leads) >= max_leads:
            print(f"\n  Reached {max_leads} leads, stopping.")
            break

        url = result["url"]
        title = result["title"][:55]
        print(f"\n  [{i}/{len(relevant_results)}] {title}")
        print(f"    {url[:75]}")

        page_text = scrape_with_contact_pages(url)
        if not page_text or len(page_text) < 100:
            print(f"    Skip (no content)")
            continue

        quick_emails = extract_emails_from_text(page_text)
        if not quick_emails:
            print(f"    Skip (no emails)")
            continue

        print(f"    {len(quick_emails)} email(s) found, extracting...")

        leads = extract_with_ollama(page_text, business_type, url)
        if not leads:
            print(f"    No leads extracted")
            continue

        leads = validate_leads(leads, source_url=url)

        new_leads = []
        for lead in leads:
            if lead["email"] not in seen_emails:
                seen_emails.add(lead["email"])
                new_leads.append(lead)
                print(f"    ✓ {lead['name'][:30]} | {lead['email']} | {lead['website']}")
            else:
                print(f"    Dup: {lead['email']}")

        all_leads.extend(new_leads)
        print(f"    Total: {len(all_leads)}")
        time.sleep(DELAY_BETWEEN_REQUESTS)

    # ------- save -------
    if not all_leads:
        print(f"\n{'=' * 60}")
        print("  No leads found.")
        print("  Tips: try 'manual' query mode, region 'all', simpler business type")
        print(f"{'=' * 60}")
        sys.exit(0)

    filename = save_to_csv(all_leads, business_type, region_input)

    with_phone = sum(1 for l in all_leads if l["phone"] != "not found")
    with_addr = sum(1 for l in all_leads if l["address"] != "not found")

    print(f"\n{'=' * 60}")
    print(f"  DONE")
    print(f"  File:         {filename}")
    print(f"  Total leads:  {len(all_leads)}")
    print(f"  With email:   {len(all_leads)} (all – required)")
    print(f"  With phone:   {with_phone}")
    print(f"  With address: {with_addr}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()

