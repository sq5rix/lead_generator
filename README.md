# lead_generator

generates lead based on local searchXNG in docker and ollama. No keys, no money, no cry

Lead Generation Search Tool
A simple terminal-based Python app that searches for businesses/leads and outputs CSV files using Ollama and SearXNG (open-source search engine via Docker).
Architecture
User (Terminal) → Python App → SearXNG (search) → Ollama (extract/structure) → CSV

How It Works
┌─────────────────────────────────────┐
│  Terminal: user types query/region   │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  SearXNG (Docker :8888)             │
│  - Searches Google, Bing, DDG       │
│  - Returns JSON results             │
│  - Region-filtered                  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Ollama (Docker :11434)             │
│  - Receives search snippets         │
│  - Extracts: name, email, phone,    │
│    address, website, description    │
│  - Returns structured JSON          │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  CSV Output                         │
│  leads_query_region_timestamp.csv   │
└─────────────────────────────────────┘

Key points:

SearXNG replaces Google Grounding — it's a meta-search engine that aggregates results from multiple engines
Ollama replaces any cloud LLM — runs locally, extracts structured data from raw search snippets
Results are batched (5 at a time) to stay within context limits
Deduplication by business name and URL
Everything runs locally, no API keys needed

lead-search/
├── docker-compose.yml
├── lead_search.py
├── requirements.txt
└── searxng/
    └── settings.yml

Running:

ollama serve
python3 venv -m venv
pip install -r requirements.txt
docker compose up -d 
python3 lead_generator.py

Happy crawling!

