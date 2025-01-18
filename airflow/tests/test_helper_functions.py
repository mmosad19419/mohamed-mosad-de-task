import os
import json
import pytest
from datetime import datetime
import requests
from unittest.mock import patch, MagicMock

from src.utils.helper_functions import parse_configs, generate_incremental_dates, fetch_data_from_api

# Unit test generate_incremental_dates
def test_generate_incremental_dates_with_large_offset():
    start_date = "2023-01-01"
    end_date = "2023-01-10"
    offset = 7
    expected_dates = [
        datetime(2023, 1, 1).date(),
        datetime(2023, 1, 8).date()
    ]
    dates = generate_incremental_dates(start_date, end_date, offset)
    assert dates == expected_dates

def test_generate_incremental_dates_no_dates():
    start_date = "2023-01-01"
    end_date = "2022-12-31"
    offset = 1
    dates = generate_incremental_dates(start_date, end_date, offset)
    assert dates == []


# Test fetch_data_from_api function
@patch('requests.get')
def test_fetch_data_from_api_success(mock_get):
    # get API credientials
    NYT_BOOKS_API_KEY, NYT_BOOKS_API_SECRET, NYT_BOOKS_API_ENDPOINT = parse_configs('./config/config.json')

    # Mock the response from the API
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "OK", "results": {"lists": []}}
    
    # Set the mock to return the above response when called
    mock_get.return_value = mock_response

    DATE = "2023-01-01"
    
    # Call the actual function
    fetch_data_from_api(NYT_BOOKS_API_KEY, DATE)

    # Check that requests.get was called with the expected URL
    mock_get.assert_called_once_with(
        f"https://api.nytimes.com/svc/books/v3/lists/overview.json?published_date={DATE}&api-key={NYT_BOOKS_API_KEY}"
    )

def test_fetch_data_from_api_failure():
    # Create a mock for requests.get simulating a Notfound error
    mock_get = MagicMock()
    mock_get.return_value.status = 404

    with MagicMock() as requests_mock:
        requests_mock.get = mock_get
        NYT_BOOKS_API_KEY = "test_api_key"
        DATE = "2023-01-01"
        with pytest.raises(Exception):
            fetch_data_from_api(NYT_BOOKS_API_KEY, DATE)