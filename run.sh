# 1. Create project directory
mkdir lead-search && cd lead-search

# 2. Create the files above (docker-compose.yml, searxng/settings.yml, lead_search.py)
mkdir -p searxng

# 3. Start services
docker compose up -d

# 4. Pull the Ollama model (wait for ollama container to be ready)
docker exec ollama ollama pull llama3.1:8b

# 5. Install Python deps
pip install requests

# 6. Run
python lead_search.py
