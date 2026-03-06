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


# ============================================================
# CONFIG
# ============================================================

SEARXNG_URL = os.getenv("SEARXNG_URL", "http://localhost:8888")

# Ollama: local (Ubuntu native) or Docker
# Native install: http://localhost:11434 (default)
# Docker: http://localhost:11434 (mapped port) or http://ollama:11434 (compose network)
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")

# Connection mode for reference
# "local" = ollama installed on Ubuntu directly (default)
# "docker" = ollama running in docker container
OLLAMA_MODE = os.getenv("OLLAMA_MODE", "local")

MAX_RESULTS = 6000

REGION_MAP = {
    "us": "us-en",
    "uk": "gb-en",
    "eu": "de-en",
    "canada": "ca-en",
    "au": "au-en",
    "all": None,
}

EU_COUNTRIES = ["de-en", "fr-fr", "es-es", "it-it", "nl-nl", "pl-pl", "se-sv"]

CSV_FIELDS = ["name", "email", "phone", "address", "website", "description", "source_url"]


# ============================================================
# OLLAMA API
# ============================================================

def ollama_get(endpoint, timeout=10):
    """GET request to Ollama API."""
    try:
        r = requests.get(f"{OLLAMA_URL}{endpoint}", timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  Ollama GET {endpoint} error: {e}")
        return None


def ollama_post(endpoint, payload, timeout=120):
    """POST request to Ollama API."""
    try:
        r = requests.post(f"{OLLAMA_URL}{endpoint}", json=payload, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  Ollama POST {endpoint} error: {e}")
        return None


def ollama_is_up():
    """Check if Ollama is reachable."""
    data = ollama_get("/api/tags", timeout=5)
    return data is not None


def ollama_list_models():
    """List all available models. Returns list of model names."""
    data = ollama_get("/api/tags")
    if not data:
        return []
    models = data.get("models", [])
    return [m.get("name", "") for m in models]


def ollama_has_model(model_name):
    """Check if a specific model is available."""
    models = ollama_list_models()
    # match with or without tag
    for m in models:
        if m == model_name or m.startswith(model_name + ":"):
            return True
    return False


def ollama_pull_model(model_name):
    """Pull a model. Returns True on success."""
    print(f"  Pulling model '{model_name}'... (this may take a while)")
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/pull",
            json={"name": model_name},
            timeout=600,
            stream=True,
        )
        r.raise_for_status()
        # stream progress
        for line in r.iter_lines():
            if line:
                try:
                    data = json.loads(line)
                    status = data.get("status", "")
                    if "pulling" in status or "download" in status.lower():
                        pct = data.get("completed", 0)
                        total = data.get("total", 0)
                        if total > 0:
                            print(f"\r  {status}: {pct}/{total} ({int(pct/total*100)}%)", end="")
                    else:
                        print(f"\r  {status}                    ", end="")
                except json.JSONDecodeError:
                    pass
        print()
        return True
    except Exception as e:
        print(f"  Pull error: {e}")
        return False


def ollama_generate(prompt, model=None, temperature=0.1):
    """Generate a response from Ollama. Returns response text or empty string."""
    model = model or OLLAMA_MODEL
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": temperature,
        },
    }
    data = ollama_post("/api/generate", payload, timeout=120)
    if data:
        return data.get("response", "")
    return ""


def ollama_model_info(model_name=None):
    """Get model info. Returns dict or None."""
    model_name = model_name or OLLAMA_MODEL
    return ollama_post("/api/show", {"name": model_name}, timeout=10)


def ollama_status():
    """Print Ollama status: connection, mode, models."""
    up = ollama_is_up()
    print(f"  Ollama URL:    {OLLAMA_URL}")
    print(f"  Ollama Mode:   {OLLAMA_MODE}")
    print(f"  Ollama Status: {'OK' if up else 'NOT REACHABLE'}")

    if up:
        models = ollama_list_models()
        print(f"  Models:        {', '.join(models) if models else 'none'}")
        has_model = ollama_has_model(OLLAMA_MODEL)
        print(f"  Target Model:  {OLLAMA_MODEL} ({'available' if has_model else 'NOT FOUND'})")
    return up


def ensure_model():
    """Make sure the configured model is available, pull if not."""
    if ollama_has_model(OLLAMA_MODEL):
        return True

    print(f"\n  Model '{OLLAMA_MODEL}' not found.")
    pull = input(f"  Pull it now? (y/n) [y]: ").strip().lower()
    if pull == "n":
        print("  Cannot continue without model.")
        return False

    return ollama_pull_model(OLLAMA_MODEL)


# ============================================================
# SERVICE CHECKS
# ============================================================

def is_searxng_up():
    """Check if SearXNG is reachable."""
    try:
        r = requests.get(f"{SEARXNG_URL}/healthz", timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def check_services():
    """Check both services, print status, return True if both OK."""
    print("\nChecking services...")

    searx_ok = is_searxng_up()
    print(f"  SearXNG: {'OK' if searx_ok else 'NOT REACHABLE at ' + SEARXNG_URL}")

    ollama_ok = ollama_status()

    if not searx_ok:
        print("\n  Start SearXNG: docker compose up -d searxng")

    if not ollama_ok:
        if OLLAMA_MODE == "local":
            print("\n  Start Ollama: ollama serve")
        else:
            print("\n  Start Ollama: docker compose up -d ollama")

    if not searx_ok or not ollama_ok:
        return False

    # make sure model is pulled
    if not ensure_model():
        return False

    return True


# ============================================================
# USER INPUT
# ============================================================

def get_query():
    """Ask user for search query."""
    print()
    query = input("What to search for: ").strip()
    if not query:
        print("No query entered.")
        return None
    return query


def get_region():
    """Ask user for region selection."""
    print()
    print("Regions: us, uk, eu, canada, au, all")
    region = input("Region [all]: ").strip().lower()
    if not region:
        region = "all"
    if region not in REGION_MAP:
        print(f"Unknown region '{region}', using 'all'.")
        region = "all"
    return region


def get_num_results():
    """Ask user how many results to fetch."""
    print()
    num_str = input("Number of results [20]: ").strip()
    if not num_str:
        return 20
    try:
        num = int(num_str)
        return max(1, min(num, MAX_RESULTS))
    except ValueError:
        print("Invalid number, using 20.")
        return 20


def collect_user_input():
    """Gather all user inputs. Returns (query, region, num_results) or None."""
    query = get_query()
    if not query:
        return None

    region = get_region()
    num_results = get_num_results()

    print()
    print(f"  Query:   {query}")
    print(f"  Region:  {region}")
    print(f"  Results: {num_results}")
    print()

    confirm = input("Proceed? (y/n) [y]: ").strip().lower()
    if confirm == "n":
        print("Cancelled.")
        return None

    return query, region, num_results


# ============================================================
# SEARCH (SearXNG)
# ============================================================

def search_searxng(query, language=None, page=1):
    """Run a single search query against SearXNG. Returns list of results."""
    params = {
        "q": query,
        "format": "json",
        "pageno": page,
    }
    if language:
        params["language"] = language

    try:
        r = requests.get(f"{SEARXNG_URL}/search", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("results", [])
    except Exception as e:
        print(f"  Search error: {e}")
        return []


def build_search_queries(base_query, region):
    """Build list of (query_string, language_code) tuples based on region."""
    contact_suffixes = ["email", "contact", "phone number", "address"]
    queries = []

    if region == "eu":
        languages = EU_COUNTRIES
    else:
        languages = [REGION_MAP.get(region)]

    for lang in languages:
        for suffix in contact_suffixes:
            full_query = f"{base_query} {suffix}"
            queries.append((full_query, lang))

    return queries


def run_search(query, region, num_results):
    """Execute all searches and return deduplicated raw results."""
    search_queries = build_search_queries(query, region)
    all_results = []
    seen_urls = set()

    print(f"Running {len(search_queries)} search queries...")

    for i, (q, lang) in enumerate(search_queries):
        print(f"  [{i + 1}/{len(search_queries)}] {q[:60]}...")
        results = search_searxng(q, language=lang)

        for r in results:
            url = r.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_results.append(r)

        time.sleep(1)

        if len(all_results) >= num_results * 3:
            break

    print(f"  Total raw results: {len(all_results)}")
    return all_results


# ============================================================
# EXTRACTION (Ollama)
# ============================================================

def format_results_for_prompt(results):
    """Convert raw search results into a text block for the LLM."""
    lines = []
    for i, r in enumerate(results, 1):
        title = r.get("title", "N/A")
        url = r.get("url", "N/A")
        snippet = r.get("content", "N/A")
        lines.append(f"Result {i}:")
        lines.append(f"  Title: {title}")
        lines.append(f"  URL: {url}")
        lines.append(f"  Snippet: {snippet}")
        lines.append("")
    return "\n".join(lines)


def build_extraction_prompt(results_text):
    """Build the prompt that asks Ollama to extract leads."""
    return f"""Extract business leads from these search results.
For each business found, extract:
- name: business name
- email: email address (or "not found")
- phone: phone number (or "not found")
- address: physical address (or "not found")
- website: website URL
- description: one line description

Return ONLY a JSON array. No explanation. No markdown.
Example: [{{"name":"Shop A","email":"a@b.com","phone":"123","address":"1 Main St","website":"http://shop.com","description":"A book shop"}}]

Search results:
{results_text}"""


def parse_llm_json(response_text):
    """Try to parse JSON array from LLM response. Returns list of dicts."""
    text = response_text.strip()

    start = text.find("[")
    end = text.rfind("]")

    if start == -1 or end == -1:
        return []

    json_str = text[start:end + 1]

    try:
        data = json.loads(json_str)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError:
        pass

    return []


def chunk_list(lst, size):
    """Split a list into chunks of given size."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def extract_leads(raw_results):
    """Send results to Ollama in batches and extract structured leads."""
    all_leads = []
    batches = list(chunk_list(raw_results, 5))

    print(f"Extracting leads from {len(raw_results)} results ({len(batches)} batches)...")

    for i, batch in enumerate(batches):
        print(f"  Batch [{i + 1}/{len(batches)}]...")

        results_text = format_results_for_prompt(batch)
        prompt = build_extraction_prompt(results_text)
        response = ollama_generate(prompt)
        leads = parse_llm_json(response)

        for lead in leads:
            if "source_url" not in lead:
                lead["source_url"] = batch[0].get("url", "") if batch else ""

        all_leads.extend(leads)
        print(f"    Found {len(leads)} leads")

    return all_leads


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate_leads(leads):
    """Remove duplicate leads by name (case-insensitive)."""
    seen = set()
    unique = []

    for lead in leads:
        name = lead.get("name", "").strip().lower()
        if name and name not in seen:
            seen.add(name)
            unique.append(lead)

    removed = len(leads) - len(unique)
    if removed > 0:
        print(f"  Removed {removed} duplicates")

    return unique


# ============================================================
# CSV OUTPUT
# ============================================================

def build_filename(query, region):
    """Generate a CSV filename from query and region."""
    safe_query = query[:30].replace(" ", "_").replace("/", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"leads_{safe_query}_{region}_{timestamp}.csv"


def save_csv(leads, query, region):
    """Save leads to a CSV file. Returns filename or None."""
    if not leads:
        print("No leads to save.")
        return None

    filename = build_filename(query, region)

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead)

    print(f"  Saved {len(leads)} leads to {filename}")
    return filename


# ============================================================
# STATS / SUMMARY
# ============================================================

def print_summary(leads, filename):
    """Print a summary of the results."""
    print()
    print("=" * 60)
    print("  DONE")

    if not leads or not filename:
        print("  No leads found.")
        print("=" * 60)
        return

    print(f"  File:        {filename}")
    print(f"  Total leads: {len(leads)}")

    emails = sum(1 for l in leads if l.get("email", "").lower() not in ("", "not found"))
    phones = sum(1 for l in leads if l.get("phone", "").lower() not in ("", "not found"))

    print(f"  With email:  {emails}")
    print(f"  With phone:  {phones}")
    print("=" * 60)


# ============================================================
# MAIN
# ============================================================

def run_pipeline():
    """Run one full search → extract → save cycle."""
    user_input = collect_user_input()
    if not user_input:
        return

    query, region, num_results = user_input

    print()
    print("[1/3] Searching...")
    raw_results = run_search(query, region, num_results)

    if not raw_results:
        print("No search results found. Try a different query.")
        return

    print()
    print("[2/3] Extracting leads with Ollama...")
    leads = extract_leads(raw_results)
    leads = deduplicate_leads(leads)

    print()
    print("[3/3] Saving CSV...")
    filename = save_csv(leads, query, region)

    print_summary(leads, filename)


def main():
    """Entry point with service check and loop."""
    print()
    print("=" * 60)
    print("  LEAD SEARCH TOOL")
    print("  SearXNG + Ollama → CSV")
    print("=" * 60)

    if not check_services():
        sys.exit(1)

    while True:
        run_pipeline()

        print()
        again = input("Search again? (y/n) [n]: ").strip().lower()
        if again != "y":
            print("Bye.")
            break


if __name__ == "__main__":
    main()


