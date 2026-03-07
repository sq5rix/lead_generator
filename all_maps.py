#!/usr/bin/env python3
"""US-wide lead generator - searches city by city"""

import csv
import json
import re
import time
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus, urlparse, urljoin
from datetime import datetime
from cities import US_CITIES
from prompts import generate_queries
from constants import *


def search(query):
    try:
        r = requests.get(SEARXNG, params={
            "q": query, "format": "json", "language": "en", "pageno": 1
        }, timeout=15)
        return r.json().get("results", [])
    except Exception as e:
        print(f"    Search error: {e}")
        return []


def extract_emails(text):
    emails = set(re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', text))
    return [e for e in emails if not any(x in e.lower() for x in
            ['noreply', 'no-reply', 'mailer-daemon', 'example.com', 'sentry',
             'wixpress', 'squarespace', 'shopify', 'cloudflare', '.png', '.jpg'])]


def scrape_page(url, timeout=10):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        return r.text
    except:
        return ""


def find_contact_page(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        text = a.get_text().lower()
        if any(w in href or w in text for w in ['contact', 'about', 'reach-us', 'connect']):
            link = a['href']
            if link.startswith('/'):
                link = urljoin(base_url, link)
            if link.startswith('http'):
                return link
    return None


def ask_ollama(prompt):
    try:
        r = requests.post(OLLAMA, json={
            "model": MODEL, "prompt": prompt, "stream": False,
            "options": {"temperature": 0.1}
        }, timeout=60)
        return r.json().get("response", "")
    except:
        return ""


def extract_lead_info(title, url, text, emails):
    prompt = f"""Extract business contact info from this page. Return ONLY valid JSON.

Business: {title}
URL: {url}
Emails found: {', '.join(emails)}

Page text (first 2000 chars):
{text[:2000]}

Return JSON:
{{"name": "business name", "email": "best email", "phone": "phone or empty", "address": "full address or empty", "website": "domain", "description": "one line what they do"}}"""

    resp = ask_ollama(prompt)
    try:
        match = re.search(r'\{[^}]+\}', resp, re.DOTALL)
        if match:
            return json.loads(match.group())
    except:
        pass
    return None


def process_result(result, seen_domains, seen_emails, leads, city):
    url = result.get('url', '')
    title = result.get('title', '')
    if not url:
        return

    domain = urlparse(url).netloc.replace('www.', '')
    if domain in seen_domains:
        return
    seen_domains.add(domain)

    if any(s in domain for s in SKIP_DOMAINS):
        return

    print(f"    Scraping {domain}...", end=" ", flush=True)
    html = scrape_page(url)
    if not html:
        print("failed")
        return

    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    emails = extract_emails(text)

    # Try contact page if no emails found
    if not emails:
        contact_url = find_contact_page(html, url)
        if contact_url:
            contact_html = scrape_page(contact_url)
            if contact_html:
                contact_soup = BeautifulSoup(contact_html, 'html.parser')
                text = contact_soup.get_text(separator=' ', strip=True)
                emails = extract_emails(text)
                url = contact_url

    if not emails:
        print("no emails")
        return

    # Skip already seen emails
    new_emails = [e for e in emails if e.lower() not in seen_emails]
    if not new_emails:
        print("duplicate")
        return

    print(f"{len(new_emails)} email(s)")

    lead = extract_lead_info(title, url, text, new_emails)
    if lead and lead.get('email'):
        email = lead['email'].lower()
        if email not in seen_emails:
            seen_emails.add(email)
            lead['source_url'] = url
            lead['city_searched'] = city
            leads.append(lead)
            print(f"      ✓ {lead.get('name', '?')} - {lead['email']}")

    time.sleep(random.uniform(1, 2))


def save_leads(leads, filename):
    fields = ['name', 'email', 'phone', 'address', 'website', 'description', 'city_searched', 'source_url']
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for lead in leads:
            writer.writerow({k: lead.get(k, '') for k in fields})


def main():
    print("=" * 60)
    print("  US-WIDE LEAD GENERATOR")
    print(f"  {len(US_CITIES)} cities loaded")
    print("=" * 60)

    business = input("\nWhat to search for (e.g. bookstore): ").strip()
    if not business:
        print("No input. Exiting.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"leads_{business.replace(' ', '_')}_{timestamp}.csv"

    leads = []
    seen_domains = set()
    seen_emails = set()

    queries_per_city = [
        '{business} {city} email contact',
        '{business} {city} "@" contact us',
        'independent {business} {city}',
    ]

    print(f"\nSearching '{business}' across {len(US_CITIES)} cities...")
    print(f"Output: {filename}\n")

    for ci, city in enumerate(US_CITIES, 1):
        print(f"\n[{ci}/{len(US_CITIES)}] === {city} === (total leads: {len(leads)})")

        queries = generate_queries(business, city)
        
        for query in queries:
            results = search(query)
            if results:
                print(f"  '{query[:60]}' → {len(results)} results")
                for result in results:
                    process_result(result, seen_domains, seen_emails, leads, city)
            time.sleep(random.uniform(1, 2))

        # Save progress every 10 cities
        if ci % 10 == 0:
            save_leads(leads, filename)
            print(f"  💾 Progress saved: {len(leads)} leads")

    save_leads(leads, filename)
    print(f"\n{'=' * 60}")
    print(f"  DONE! {len(leads)} leads saved to {filename}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()


