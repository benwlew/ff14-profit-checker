import requests
import os
import json
from dotenv import load_dotenv

# --- Authentication ---
# For security, load your token from an environment variable.
# Replace 'YOUR_PAT' if testing locally, but use environment variables for production.
load_dotenv()

pat = os.environ.get("GITHUB_API_KEY") 

headers = {
    "Authorization": f"Bearer {pat}",
    "Accept": "application/vnd.github.v3+json",
}
print(headers)
# --- API call ---
url = "https://api.github.com/rate_limit"
try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for bad status codes
    print(json.dumps(response.json(), indent=2))


except requests.exceptions.RequestException as e:
    print(f"Error making API request: {e}")