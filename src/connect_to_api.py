# imports
import os
from datetime import datetime
import requests
import json

# Parsing the configration file
with open('./config.json') as config_file:
    configs = json.load(config_file)

# retrieve API credientials
NYT_BOOKS_API_KEY = configs["API"]["NYT_BOOKS_API_KEY"]
NYT_BOOKS_API_SECRET = configs["API"]["NYT_BOOKS_API_SECRET"]
NYT_BOOKS_API_ENDPOINT = configs["API"]["NYT_API_ENDPOINT"]







def get_lists_data(DATE=None):
    # connect to API and pull data
    URL = f"https://api.nytimes.com/svc/books/v3/lists/overview.json/{DATE}"
    response = requests.get(URL)

    if response.status_code == 200:
        data = response.json()

        with open(f"./raw_data/{DATE}.json", "w") as raw_file:
            raw_file.write(json.dump(data))






