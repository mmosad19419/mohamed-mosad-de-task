# imports
import os
from datetime import datetime, timedelta
import time
import itertools
import json
import requests

from utils.helper_functions import parse_configs, generate_incremental_dates, fetch_data_from_api

# get API credientials
NYT_BOOKS_API_KEY, NYT_BOOKS_API_SECRET, NYT_BOOKS_API_ENDPOINT = parse_configs('./config.json')


START_DATE = '2022-05-29'
END_DATE = '2023-12-31'
OFFSET = 7

dates = generate_incremental_dates(START_DATE, END_DATE, OFFSET)

for date in dates:
    fetch_data_from_api(NYT_BOOKS_API_KEY, date)
    time.sleep(12)