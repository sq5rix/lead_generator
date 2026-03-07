#!/usr/bin/env python3
"""Lead generator using Google Maps data via SearXNG + direct scraping"""

import csv
import json
import re
import time
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import sys

SEARXNG = "http://localhost:8888/search"
OLLAMA = "http://localhost:11434/api/generate"
MODEL = "llama3.1:8b"
OUTPUT = "leads.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

def search(query, num=30):
    """Search via SearXNG"""
    results = []
    try:
        r = requests.get(SEARXNG, params={
            "q": query,
            "format": "json",
            "language": "en",
            "pageno": 1
        }, timeout=15)
        data = r.json()
        results = data.get("results", [])
    except Exception as e:
        print(f"  Search error: {e}")
    return results

def extract_emails(text):
    """Pull emails from text"""
    return list(set(re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', text)))

def scrape_page(url, timeout=10):
    """Fetch a page, return text"""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        return r.text
    except:
        return ""

def find_contact_page(html, base_url):
    """Find contact/about page link"""
    soup = BeautifulSoup(html, 'html.parser')
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        text = a.get_text().lower()
        if any(w in href or w in text for w in ['contact', 'about', 'reach-us', 'connect']):
            link = a['href']
            if link.startswith('/'):
                from urllib.parse import urljoin
                link = urljoin(base_url, link)
            if link.startswith('http'):
                return link
    return None

def ask_ollama(prompt):
    """Query Ollama"""
    try:
        r = requests.post(OLLAMA, json={
            "model": MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.1}
        }, timeout=60)
        return r.json().get("response", "")
    except:
        return ""

def extract_lead_info(business_name, url, page_text, emails):
    """Use Ollama to structure the lead"""
    prompt = f"""Extract business contact info from this page. Return ONLY valid JSON.

Business: {business_name}
URL: {url}
Emails found: {', '.join(emails)}

Page text (first 2000 chars):
{page_text[:2000]}

Return JSON:
{{"name": "business name", "email": "best email", "phone": "phone or empty", "address": "address or empty", "website": "domain", "description": "one line description"}}"""

    resp = ask_ollama(prompt)
    try:
        # Find JSON in response
        match = re.search(r'\{[^}]+\}', resp, re.DOTALL)
        if match:
            return json.loads(match.group())
    except:
        pass
    return None

def generate_queries(business_type, location):
    """Generate search queries targeting email-rich pages"""
    queries = [
        # Directory/list pages that aggregate contact info
        f'{business_type} {location} email contact directory',
        f'{business_type} {location} "email" "@"',
        f'list of {business_type}s in {location} with contact information',
        f'{business_type} {location} site:yelp.com OR site:yellowpages.com',
        # Direct business sites
        f'{business_type} {location} contact us',
        f'independent {business_type} {location}',
        # Chamber of commerce / local directories
        f'{business_type} {location} chamber of commerce directory',
        f'{business_type}s near {location} contact email phone',
    ]
    return queries

def process_result(result, seen_domains, leads):
    """Process a single search result"""
    url = result.get('url', '')
    title = result.get('title', '')
    
    if not url:
        return
    
    # Skip obvious junk domains
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.replace('www.', '')
    
    if domain in seen_domains:
        return
    seen_domains.add(domain)
    
    # Skip mega platforms (we want actual business sites or small directories)
    skip = ['amazon.', 'facebook.com', 'instagram.com', 'twitter.com', 
            'tiktok.com', 'youtube.com', 'wikipedia.org', 'pinterest.com']
    if any(s in domain for s in skip):
        return

    print(f"  Scraping: {domain}...", end=" ", flush=True)
    
    html = scrape_page(url)
    if not html:
        print("failed")
        return
    
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    emails = extract_emails(text)
    
    # Also check contact page
    if not emails:
        contact_url = find_contact_page(html, url)
        if contact_url:
            contact_html = scrape_page(contact_url)
            if contact_html:
                contact_soup = BeautifulSoup(contact_html, 'html.parser')
                text = contact_soup.get_text(separator=' ', strip=True)
                emails = extract_emails(text)
                url = contact_url
    
    # Filter out generic/junk emails
    emails = [e for e in emails if not any(x in e.lower() for x in 
              ['noreply', 'no-reply', 'mailer-daemon', 'example.com', 'sentry.io',
               'wixpress', 'squarespace', 'shopify', 'cloudflare'])]
    
    if not emails:
        print("no emails")
        return
    
    print(f"found {len(emails)} email(s)")
    
    # Use Ollama to structure it
    lead = extract_lead_info(title, url, text, emails)
    if lead and lead.get('email'):
        lead['source_url'] = url
        leads.append(lead)
        print(f"    ✓ {lead.get('name', '?')} - {lead.get('email', '?')}")
    
    time.sleep(random.uniform(1, 3))

def main():
    print("=" * 60)
    print("  LEAD GENERATOR")
    print("=" * 60)
    
    business_type = input("\nBusiness type (e.g. bookstore): ").strip()
    location = input("Location (e.g. Portland OR): ").strip()
    
    queries = generate_queries(business_type, location)
    
    print(f"\nRunning {len(queries)} searches...")
    
    leads = []
    seen_domains = set()
    
    for i, query in enumerate(queries, 1):
        print(f"\n[{i}/{len(queries)}] {query}")
        results = search(query)
        print(f"  Got {len(results)} results")
        
        for result in results:
            process_result(result, seen_domains, leads)
        
        time.sleep(2)
    
    # Write CSV
    if leads:
        fields = ['name', 'email', 'phone', 'address', 'website', 'description', 'source_url']
        with open(OUTPUT, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for lead in leads:
                writer.writerow({k: lead.get(k, '') for k in fields})
        print(f"\n✓ Saved {len(leads)} leads to {OUTPUT}")
    else:
        print("\nNo leads found.")

if __name__ == "__main__":
    main()



