#!/usr/bin/env python3
"""
lead_search.py - Terminal-based lead generation tool
Uses SearXNG for search + Ollama for extraction → CSV output
"""

import requests
import json
import csv
import time
import sys
import os
from datetime import datetime


# --- Config ---
SEARXNG_URL = os.getenv("SEARXNG_URL", "http://localhost:8888")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")

REGION_MAP = {
    "us": "us-en",
    "uk": "gb-en",
    "eu": "de-en",  # searxng uses country codes, eu defaults to germany
    "canada": "ca-en",
    "au": "au-en",
    "all": None,
}

# Specific EU country codes for broader EU search
EU_COUNTRIES = ["de-en", "fr-fr", "es-es", "it-it", "nl-nl", "pl-pl", "se-sv"]


def check_services():
    """Check if SearXNG and Ollama are running."""
    print("Checking services...")

    try:
        r = requests.get(f"{SEARXNG_URL}/healthz", timeout=5)
        print(f"  SearXNG: OK")
    except Exception:
        print(f"  SearXNG: NOT REACHABLE at {SEARXNG_URL}")
        print(f"  Run: docker compose up -d")
        return False

    try:
        r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
        models = [m["name"] for m in r.json().get("models", [])]
        if not any(OLLAMA_MODEL.split(":")[0] in m for m in models):
            print(f"  Ollama: running but model '{OLLAMA_MODEL}' not found")
            print(f"  Run: docker exec ollama ollama pull {OLLAMA_MODEL}")
            return False
        print(f"  Ollama: OK (model: {OLLAMA_MODEL})")
    except Exception:
        print(f"  Ollama: NOT REACHABLE at {OLLAMA_URL}")
        print(f"  Run: docker compose up -d")
        return False

    return True


def search_searxng(query, region=None, num_results=10):
    """Search using SearXNG JSON API."""
    all_results = []
    pages_needed = (num_results // 10) + 1

    regions = [region] if region else [None]

    # If EU, search multiple countries
    if region == "eu":
        regions = EU_COUNTRIES

    for reg in regions:
        for page in range(1, pages_needed + 1):
            params = {
                "q": query,
                "format": "json",
                "pageno": page,
                "engines": "google,bing,duckduckgo",
            }
            if reg:
                params["language"] = reg

            try:
                r = requests.get(
                    f"{SEARXNG_URL}/search",
                    params=params,
                    timeout=30,
                )
                r.raise_for_status()
                data = r.json()
                results = data.get("results", [])
                all_results.extend(results)
                print(f"  Fetched page {page} region={reg or 'all'}: {len(results)} results")
                time.sleep(1)  # be nice to search engines
            except Exception as e:
                print(f"  Search error (page {page}, region {reg}): {e}")
                continue

            if len(all_results) >= num_results:
                break

        if len(all_results) >= num_results:
            break

    # Deduplicate by URL
    seen = set()
    unique = []
    for r in all_results:
        url = r.get("url", "")
        if url not in seen:
            seen.add(url)
            unique.append(r)

    return unique[:num_results]


def extract_with_ollama(search_results, original_query, region):
    """Send search results to Ollama to extract structured lead data."""

    # Prepare search data for the LLM
    search_text = ""
    for i, r in enumerate(search_results, 1):
        search_text += f"""
--- Result {i} ---
Title: {r.get('title', 'N/A')}
URL: {r.get('url', 'N/A')}
Snippet: {r.get('content', 'N/A')}
"""

    prompt = f"""You are a lead generation assistant. I searched for: "{original_query}" in region: "{region}".

Below are search results. Extract business/organization leads from them.

For EACH lead found, extract:
- business_name: the company/organization name
- website: their website URL
- email: email address if visible (or "not found")
- phone: phone number if visible (or "not found")  
- address: physical address if visible (or "not found")
- description: brief description of what they do
- source_url: the URL where this info was found

Return ONLY a valid JSON array. No markdown, no explanation. Just the JSON array.
Example format:
[
  {{
    "business_name": "Example Co",
    "website": "https://example.com",
    "email": "info@example.com",
    "phone": "+1-555-0100",
    "address": "123 Main St, City, State",
    "description": "A bookstore chain",
    "source_url": "https://source.com/page"
  }}
]

If a result is not a relevant business lead, skip it.

SEARCH RESULTS:
{search_text}

JSON ARRAY:"""

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 4096,
                },
            },
            timeout=120,
        )
        r.raise_for_status()
        response_text = r.json().get("response", "")

        # Try to parse JSON from response
        # Sometimes LLM wraps it in ```json blocks
        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines)

        leads = json.loads(cleaned)
        return leads

    except json.JSONDecodeError as e:
        print(f"  Warning: Could not parse LLM response as JSON: {e}")
        print(f"  Raw response (first 500 chars): {response_text[:500]}")
        return []
    except Exception as e:
        print(f"  Ollama error: {e}")
        return []


def process_in_batches(search_results, query, region, batch_size=5):
    """Process search results in batches to avoid token limits."""
    all_leads = []
    total = len(search_results)

    for i in range(0, total, batch_size):
        batch = search_results[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total + batch_size - 1) // batch_size
        print(f"  Processing batch {batch_num}/{total_batches} ({len(batch)} results)...")

        leads = extract_with_ollama(batch, query, region)
        if leads:
            all_leads.extend(leads)
            print(f"    Extracted {len(leads)} leads")
        else:
            print(f"    No leads extracted from this batch")

        time.sleep(0.5)

    return all_leads


def save_csv(leads, query, region):
    """Save leads to CSV file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_query = "".join(c if c.isalnum() or c == " " else "_" for c in query)
    safe_query = safe_query.replace(" ", "_")[:50]
    filename = f"leads_{safe_query}_{region}_{timestamp}.csv"

    if not leads:
        print("No leads to save.")
        return None

    fieldnames = [
        "business_name",
        "website",
        "email",
        "phone",
        "address",
        "description",
        "source_url",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead)

    print(f"Saved {len(leads)} leads to: {filename}")
    return filename


def deduplicate_leads(leads):
    """Remove duplicate leads by business name or website."""
    seen_names = set()
    seen_urls = set()
    unique = []

    for lead in leads:
        name = lead.get("business_name", "").lower().strip()
        url = lead.get("website", "").lower().strip()

        if name and name in seen_names:
            continue
        if url and url != "not found" and url in seen_urls:
            continue

        if name:
            seen_names.add(name)
        if url and url != "not found":
            seen_urls.add(url)
        unique.append(lead)

    return unique


def main():
    print("=" * 60)
    print("  LEAD GENERATION SEARCH TOOL")
    print("  (SearXNG + Ollama - fully open source)")
    print("=" * 60)
    print()

    if not check_services():
        print("\nFix the above issues and try again.")
        sys.exit(1)

    print()

    # --- Get user input ---
    query = input("Search query (e.g. 'libraries with a person buying books email'): ").strip()
    if not query:
        print("No query entered. Exiting.")
        sys.exit(1)

    print()
    print("Regions: us, uk, eu, canada, au, all")
    region = input("Region [all]: ").strip().lower()
    if not region:
        region = "all"
    if region not in REGION_MAP:
        print(f"Unknown region '{region}'. Using 'all'.")
        region = "all"

    print()
    num_str = input("Number of search results to fetch [20]: ").strip()
    try:
        num_results = int(num_str) if num_str else 20
    except ValueError:
        num_results = 20

    print()
    print("-" * 60)
    print(f"Query:   {query}")
    print(f"Region:  {region}")
    print(f"Results: {num_results}")
    print("-" * 60)
    print()

    # --- Search ---
    print("[1/3] Searching...")
    searx_region = REGION_MAP.get(region)
    if region == "eu":
        searx_region = "eu"  # special handling in search function

    search_results = search_searxng(query, searx_region, num_results)
    print(f"  Got {len(search_results)} unique results")

    if not search_results:
        print("No search results found. Try a different query.")
        sys.exit(1)

    # --- Extract ---
    print()
    print("[2/3] Extracting leads with Ollama...")
    leads = process_in_batches(search_results, query, region)
    leads = deduplicate_leads(leads)
    print(f"  Total unique leads: {len(leads)}")

    # --- Save ---
    print()
    print("[3/3] Saving CSV...")
    filename = save_csv(leads, query, region)

    # --- Summary ---
    print()
    print("=" * 60)
    print("  DONE")
    if filename:
        print(f"  File: {filename}")
        print(f"  Leads found: {len(leads)}")

        # Quick preview
        emails_found = sum(
            1 for l in leads if l.get("email") and l["email"].lower() != "not found"
        )
        print(f"  With emails: {emails_found}")
    print("=" * 60)

    # Ask if user wants another search
    print()
    again = input("Search again? (y/n) [n]: ").strip().lower()
    if again == "y":
        main()


if __name__ == "__main__":
    main()
