# imports
import os
from datetime import datetime, timedelta
import itertools
import jsons
import requests


# Parsing the configration file
def parse_configs(config_file_path):
    """
    Parses the configuration file and retrieves API credentials.

    Args:
        config_file_path (str): Path to the configuration file (JSON format).

    Returns:
        tuple: A tuple containing the API key, API secret, and API endpoint.
    """

    with open('./config.json') as config_file:
        configs = json.load(config_file)

    # retrieve API credientials
    NYT_BOOKS_API_KEY = configs["API"]["NYT_BOOKS_API_KEY"]
    NYT_BOOKS_API_SECRET = configs["API"]["NYT_BOOKS_API_SECRET"]
    NYT_BOOKS_API_ENDPOINT = configs["API"]["NYT_API_ENDPOINT"]

    return NYT_BOOKS_API_KEY, NYT_BOOKS_API_SECRET, NYT_BOOKS_API_ENDPOINT


def generate_incremental_dates(start_date, end_date, offset):
    """
    Generates a list of dates starting from `start_date`, incremented by `offset`, 
    until the `end_date` is reached.

    Args:
        start_date (str): The starting date in the format "YYYY-MM-DD".
        end_date (str): The ending date in the format "YYYY-MM-DD".
        offset (int): The number of days to increment for each date in the range.

    Returns:
        list: A list of `datetime` objects representing the dates from `start_date`
              to `end_date`, incremented by `offset` days.
    """

    # format dates
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # init date variable
    date = start_date

    # Init dates list
    dates = []

    # loop to get all required dates in specified range
    while date <= end_date:
        dates.append(date)

        date += timedelta(days=offset)

    return dates


# Define a function to fetch data from API
def fetch_data_from_api(NYT_BOOKS_API_KEY, DATE):
    """
    Fetches book data from the New York Times Books API for a specific date and 
    saves the response as a JSON file in the `raw_data` folder.

    Args:
        NYT_BOOKS_API_KEY (str): The API key for accessing the New York Times Books API.
        DATE (str): The date for which to fetch book data, in the format "YYYY-MM-DD".

    Returns:
        None: This function does not return any value. It saves the data to a file.
    """

    # connect to API and pull data
    URL = f"https://api.nytimes.com/svc/books/v3/lists/overview.json?published_date={DATE}&api-key={NYT_BOOKS_API_KEY}"
    response = requests.get(URL)

    if response.status == "OK":
        # Save the data to a JSON file in the raw_data folder
        os.makedirs("./raw_data", exist_ok=True)

        with open(f"./raw_data/{DATE}.json", "w") as raw_file:
            json.dump(response.json(), raw_file)

       