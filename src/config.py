import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("LIQUIPEDIA_API_KEY")

BASE_URLS = {
    "tournament": "https://api.liquipedia.net/api/v3/tournament",
}

HEADERS = {
    "accept": "application/json",
    "authorization": f"Apikey {API_KEY}",
}
