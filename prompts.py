from constants import *
import requests

def generate_queries(business_type, city, num=5):
    prompt = f"""Generate {num} search queries to find independent {business_type} businesses in {city} that show their email address on their website.

Rules:
- Queries should find the actual business websites, NOT directories, chambers of commerce, news articles, or university pages
- Use terms like "contact us" "@gmail.com" "@yahoo.com" or "email" to target pages with emails visible
- Include the city name in every query
- Mix approaches: some with "independent", some with "local", some targeting contact pages directly

Return ONLY the queries, one per line, no numbering, no explanation."""

    try:
        r = requests.post(f"{OLLAMA.replace('/api/generate','/api/chat')}", json={
            "model": MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "options": {"temperature": 0.8, "num_predict": 500}
        }, timeout=60)
        lines = r.json()["message"]["content"].strip().split("\n")


    except Exception as e:
        print(f"  Ollama error: {e}")

    # Fallback
    return [
        f'independent {business_type} {city} contact us email',
        f'local {business_type} {city} "@" contact',
        f'{business_type} {city} site:.com contact email',
    ]

