#!/usr/bin/env python3
"""
Lead Scraper – Terminal-based, uses SearXNG + Ollama (local, open-source only).
Searches for businesses, scrapes pages, extracts leads with EMAIL REQUIRED.
"""

import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import urlparse

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
MAX_CONTENT_LENGTH = 8000  # chars to send to Ollama
DELAY_BETWEEN_REQUESTS = 2

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Domains to always skip
SKIP_DOMAINS = [
    "listsxpanders.com", "educationdatalists.com", "libro.fm",
    "oxfordonlinepractice.com", "youtube.com", "facebook.com",
    "yelp.com", "yellowpages.com", "linkedin.com", "wikipedia.org",
    "amazon.com", "ebay.com", "reddit.com", "twitter.com",
    "instagram.com", "tiktok.com", "pinterest.com", "tripadvisor.com",
    "mapquest.com", "apple.com", "google.com", "googleapis.com",
    "gov", "bbb.org", "crunchbase.com", "zoominfo.com",
    "dnb.com", "hoovers.com", "manta.com", "infobel.com",
    "superpages.com", "whitepages.com", "chamberofcommerce.com",
    "graywolfpress.org", "candlewick.com", "chroniclebooks.com",
    "ucpress.edu", "randomhouse.com", "penguinrandomhouse.com",
    "harpercollins.com", "simonandschuster.com", "macmillan.com",
    "hachette.com", "mpsvirginia.com",
    "disney.com", "oxfordlearnersdictionaries.com", "independent.com",
    "milkweed.org", "bookstores.com", "greatbooks.org",
    "barnesandnoble.com", "bookshop.org", "thriftbooks.com",
    "abebooks.com", "powells.com", "betterworldbooks.com",
]

# Data broker phrases in titles/descriptions
JUNK_PHRASES = [
    "mailing list", "email list", "buy leads", "data provider",
    "b2b database", "marketing list", "lead generation",
    "bulk email", "sales leads", "business database",
    "publisher", "university press", "publishing house",
    "newspaper", "dictionary", "online bookstore", "ebook",
    "publishing", "press release",
]

REGION_MAP = {
    "us": "us",
    "uk": "gb",
    "eu": "eu",
    "canada": "ca",
    "au": "au",
    "all": None,
}


# ============================================================
# SEARXNG SEARCH
# ============================================================
def search_searxng(query, region=None, max_results=30):
    """Search SearXNG and return list of {url, title, snippet}."""
    results = []
    seen_urls = set()
    pages_to_try = max(1, max_results // 10)

    for page in range(1, pages_to_try + 1):
        params = {
            "q": query,
            "format": "json",
            "categories": "general",
            "pageno": page,
            "language": "en",
        }
        if region:
            params["language"] = f"en-{region.upper()}"
            params["region"] = region

        try:
            r = requests.get(SEARXNG_URL, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  [!] SearXNG error page {page}: {e}")
            break

        page_results = data.get("results", [])
        if not page_results:
            break

        for item in page_results:
            url = item.get("url", "")
            if not url or url in seen_urls:
                continue
            if should_skip_url(url):
                continue

            title = item.get("title", "")
            snippet = item.get("content", "")

            # Skip data broker junk
            combined = (title + " " + snippet).lower()
            if any(phrase in combined for phrase in JUNK_PHRASES):
                print(f"  Skipped data broker: {title[:50]}")
                continue

            seen_urls.add(url)
            results.append({
                "url": url,
                "title": title,
                "snippet": snippet,
            })

        time.sleep(DELAY_BETWEEN_REQUESTS)

    return results[:max_results]

def build_search_queries(business_type, location):
    """Queries targeting actual local stores with contact info."""
    queries = [
        f'"{business_type}" {location} "contact us" email site:.com',
        f'independent {business_type} {location} "@gmail.com" OR "@yahoo.com"',
        f'local {business_type} shop {location} email phone address',
        f'indie {business_type} {location} "info@" OR "contact@"',
        f'{business_type} {location} "hours" "email" "phone" -publisher -press -wholesale',
        f'neighborhood {business_type} store {location} email',
        f'used {business_type} {location} contact email',
    ]
    return queries


def should_skip_url(url):
    """Skip known junk domains."""
    url_lower = url.lower()
    for domain in SKIP_DOMAINS:
        if domain in url_lower:
            return True
    return False


# ============================================================
# WEB SCRAPING
# ============================================================
def scrape_page(url):
    """Scrape a single page, return cleaned text."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=SCRAPE_TIMEOUT, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Remove scripts, styles, nav, footer junk
        for tag in soup(["script", "style", "nav", "header", "footer", "noscript", "svg", "img"]):
            tag.decompose()

        text = soup.get_text(separator="\n", strip=True)
        # Collapse whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        return text[:MAX_CONTENT_LENGTH]
    except Exception as e:
        return ""


def get_contact_urls(base_url):
    """Generate possible contact page URLs from a base URL."""
    try:
        parsed = urlparse(base_url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
    except:
        return [base_url]

    contact_paths = [
        "", "/contact", "/contact-us", "/about", "/about-us",
        "/info", "/connect", "/reach-us", "/get-in-touch",
    ]

    urls = []
    for path in contact_paths:
        urls.append(f"{domain}{path}")
    return urls


def scrape_with_contact_pages(base_url):
    """Scrape the main page + contact/about pages, combine text."""
    all_text = []
    urls_tried = set()

    contact_urls = get_contact_urls(base_url)

    for url in contact_urls:
        if url in urls_tried:
            continue
        urls_tried.add(url)

        text = scrape_page(url)
        if text and len(text) > 100:
            all_text.append(f"--- PAGE: {url} ---\n{text}")

        time.sleep(0.5)

        # Don't scrape too many pages
        if len(all_text) >= 3:
            break

    combined = "\n\n".join(all_text)
    return combined[:MAX_CONTENT_LENGTH * 2]


def extract_emails_from_text(text):
    """Regex extract emails from raw text as fallback."""
    if not text:
        return []
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(pattern, text)
    # Filter junk emails
    junk = ["example.com", "sentry.io", "wixpress.com", "w3.org", "schema.org",
            "googleapis.com", "cloudflare.com", "wordpress.com", "gravatar.com",
            ".png", ".jpg", ".gif", ".svg", ".css", ".js"]
    cleaned = []
    for e in emails:
        e_lower = e.lower()
        if any(j in e_lower for j in junk):
            continue
        if e_lower not in cleaned:
            cleaned.append(e_lower)
    return cleaned


# ============================================================
# OLLAMA EXTRACTION
# ============================================================
def extract_with_ollama(page_text, business_type, source_url):
    """Send scraped text to Ollama to extract structured lead data."""

    # First do regex email extraction as backup
    regex_emails = extract_emails_from_text(page_text)

    email_hint = ""
    if regex_emails:
        email_hint = f"\nEmails found on page: {', '.join(regex_emails[:10])}"

    prompt = f"""Extract business contact information from this webpage text.
I need: business name, email, phone, physical address, website, short description.

Business type I'm looking for: {business_type}
Source URL: {source_url}
{email_hint}

RULES:
- Only extract REAL businesses, not directories or data brokers
- Email is REQUIRED - if no email found, return empty JSON array
- Each business must be a separate entry
- Return ONLY valid JSON array, nothing else
- If multiple businesses found, return all of them

Return format:
[
  {{
    "name": "Business Name",
    "email": "real@email.com",
    "phone": "phone number or not found",
    "address": "full address or not found",
    "website": "website.com",
    "description": "what they do in 10 words or less"
  }}
]

If NO business with email found, return: []

PAGE TEXT:
{page_text[:MAX_CONTENT_LENGTH]}
"""

    try:
        r = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 2000,
                }
            },
            timeout=120,
        )
        r.raise_for_status()
        response_text = r.json().get("response", "")
        return parse_ollama_response(response_text, regex_emails, source_url)

    except Exception as e:
        print(f"  [!] Ollama error: {e}")
        # Fallback: if we found emails via regex, create basic leads
        if regex_emails:
            return create_fallback_leads(regex_emails, source_url)
        return []


def parse_ollama_response(text, regex_emails, source_url):
    """Parse JSON from Ollama response."""
    # Find JSON array in response
    text = text.strip()

    # Try to find JSON array
    start = text.find('[')
    end = text.rfind(']')

    if start == -1 or end == -1:
        if regex_emails:
            return create_fallback_leads(regex_emails, source_url)
        return []

    json_str = text[start:end + 1]

    try:
        leads = json.loads(json_str)
        if isinstance(leads, list):
            return leads
    except json.JSONDecodeError:
        # Try to fix common JSON issues
        json_str = json_str.replace("'", '"')
        json_str = re.sub(r',\s*]', ']', json_str)
        json_str = re.sub(r',\s*}', '}', json_str)
        try:
            leads = json.loads(json_str)
            if isinstance(leads, list):
                return leads
        except:
            pass

    if regex_emails:
        return create_fallback_leads(regex_emails, source_url)
    return []


def create_fallback_leads(emails, source_url, business_type=""):
    """Create basic lead entries from regex-extracted emails.
    Only keep ONE email per domain, skip if source doesn't match business type."""
    domain = clean_website(source_url)
    
    # Filter: keep only emails matching the source domain (skip random @randomhouse.com etc)
    domain_emails = [e for e in emails if domain and domain.split('.')[0] in e]
    # If none match domain, just take the first one
    if not domain_emails:
        domain_emails = emails[:1]
    
    # Only keep first email per source (one lead per business)
    email = domain_emails[0] if domain_emails else None
    if not email:
        return []
    
    return [{
        "name": domain or "Unknown",
        "email": email,
        "phone": "not found",
        "address": "not found",
        "website": domain,
        "description": "Auto-extracted from page",
    }]


# ============================================================
# LEAD VALIDATION & CLEANING
# ============================================================
def clean_website(url):
    """Extract clean domain from URL."""
    if not url or url == "not found":
        return "not found"
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^www\.', '', url)
    url = url.split('/')[0]
    url = url.split('?')[0]
    url = url.split('#')[0]
    return url.strip().lower()


def is_valid_email(email):
    """Check if email looks real."""
    if not email or email == "not found":
        return False
    email = email.strip().lower()
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False
    # Skip junk emails
    junk_domains = [
        "example.com", "test.com", "sentry.io", "wixpress.com",
        "w3.org", "schema.org", "googleapis.com", "cloudflare.com",
        "wordpress.com", "gravatar.com", "yoursite.com", "email.com",
        "domain.com", "website.com", "company.com",
    ]
    domain = email.split('@')[1]
    if domain in junk_domains:
        return False
    return True


def validate_leads(leads, source_url=""):
    """Filter and clean leads. Email is REQUIRED."""
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

        # MUST have valid email
        if not is_valid_email(email):
            print(f"    Dropped (no valid email): {name}")
            continue

        # Deduplicate by email
        if email in seen_emails:
            print(f"    Dropped (duplicate): {email}")
            continue
        seen_emails.add(email)

        # Clean website to domain only
        website = clean_website(website)
        if website == "not found" and source_url:
            website = clean_website(source_url)

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
    """Save leads to CSV file."""
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
    print("  LEAD SCRAPER")
    print("  SearXNG + Ollama (100% local & open source)")
    print("=" * 60)

    # Check services
    print("\nChecking services...")
    try:
        requests.get(SEARXNG_URL.replace("/search", "/"), timeout=5)
        print("  ✓ SearXNG OK")
    except:
        print("  ✗ SearXNG not reachable at", SEARXNG_URL)
        print("    Start it: docker-compose up -d")
        sys.exit(1)

    try:
        requests.get(OLLAMA_URL.replace("/api/generate", "/"), timeout=5)
        print("  ✓ Ollama OK")
    except:
        print("  ✗ Ollama not reachable at", OLLAMA_URL)
        print("    Start it: ollama serve")
        sys.exit(1)

    # User input
    print()
    business_type = input("What to search for (e.g. 'bookstores'): ").strip()
    if not business_type:
        print("Need a search term!")
        sys.exit(1)

    print(f"\nRegions: us, uk, eu, canada, au, all")
    region_input = input("Region [us]: ").strip().lower() or "us"
    region = REGION_MAP.get(region_input, "us")

    max_leads = input("Max leads to find [50]: ").strip()
    max_leads = int(max_leads) if max_leads.isdigit() else 50

    # Location text for queries
    location_names = {
        "us": "United States",
        "gb": "United Kingdom",
        "eu": "Europe",
        "ca": "Canada",
        "au": "Australia",
        None: "",
    }
    location = location_names.get(region, region_input)

    print(f"\n{'=' * 60}")
    print(f"  Searching: {business_type}")
    print(f"  Region:    {location or 'worldwide'}")
    print(f"  Max leads: {max_leads}")
    print(f"{'=' * 60}\n")

    # Build and run queries
    queries = build_search_queries(business_type, location)
    all_search_results = []
    seen_urls = set()

    for i, query in enumerate(queries, 1):
        print(f"[{i}/{len(queries)}] Searching: {query[:70]}...")
        results = search_searxng(query, region, max_results=20)
        for r in results:
            if r["url"] not in seen_urls:
                seen_urls.add(r["url"])
                all_search_results.append(r)
        print(f"  Found {len(results)} results ({len(all_search_results)} total unique)")
        time.sleep(DELAY_BETWEEN_REQUESTS)

    if not all_search_results:
        print("\nNo search results found. Check SearXNG config.")
        sys.exit(1)

    print(f"\nTotal unique URLs to process: {len(all_search_results)}")

    # Process each URL
    all_leads = []
    seen_emails = set()

    for i, result in enumerate(all_search_results, 1):
        if len(all_leads) >= max_leads:
            print(f"\nReached {max_leads} leads, stopping.")
            break

        url = result["url"]
        title = result["title"][:60]
        print(f"\n[{i}/{len(all_search_results)}] {title}")
        print(f"  URL: {url[:80]}")

        # Scrape main page + contact pages
        print(f"  Scraping (+ contact pages)...")
        page_text = scrape_with_contact_pages(url)

        if not page_text or len(page_text) < 100:
            print(f"  Skipped (no content)")
            continue

        # Quick check: any emails on page at all?
        quick_emails = extract_emails_from_text(page_text)
        if not quick_emails:
            print(f"  Skipped (no emails found on page)")
            continue

        print(f"  Found {len(quick_emails)} email(s) on page, extracting with Ollama...")

        # Extract with Ollama
        leads = extract_with_ollama(page_text, business_type, url)

        if not leads:
            print(f"  No leads extracted")
            continue

        # Validate
        leads = validate_leads(leads, source_url=url)

        # Deduplicate against master list
        new_leads = []
        for lead in leads:
            if lead["email"] not in seen_emails:
                seen_emails.add(lead["email"])
                new_leads.append(lead)
                print(f"  ✓ {lead['name']} | {lead['email']} | {lead['website']}")
            else:
                print(f"    Duplicate: {lead['email']}")

        all_leads.extend(new_leads)
        print(f"  Running total: {len(all_leads)} leads")

        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Save results
    if not all_leads:
        print("\n" + "=" * 60)
        print("  No leads found. Try different search terms.")
        print("=" * 60)
        sys.exit(0)

    filename = save_to_csv(all_leads, business_type, region_input)

    # Summary
    emails = sum(1 for l in all_leads if l["email"] != "not found")
    phones = sum(1 for l in all_leads if l["phone"] != "not found")
    addresses = sum(1 for l in all_leads if l["address"] != "not found")

    print(f"\n{'=' * 60}")
    print(f"  DONE")
    print(f"  File:        {filename}")
    print(f"  Total leads: {len(all_leads)}")
    print(f"  With email:  {emails}")
    print(f"  With phone:  {phones}")
    print(f"  With address:{addresses}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()


