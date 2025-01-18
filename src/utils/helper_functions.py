# imports
import logging
import os
from datetime import datetime, timedelta
import itertools
import json
import requests
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd


# Setup logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_configs(config_file_path):
    """
    Parses the configuration file and retrieves API credentials.

    Args:
        config_file_path (str): Path to the configuration file (JSON format).

    Returns:
        tuple: A tuple containing the API key, API secret, and API endpoint.
    """
    logging.info(f"Parsing configuration file: {config_file_path}")
    
    try:
        with open(config_file_path) as config_file:
            configs = json.load(config_file)

        # Retrieve API credentials
        NYT_BOOKS_API_KEY = configs["API"]["NYT_BOOKS_API_KEY"]
        NYT_BOOKS_API_SECRET = configs["API"]["NYT_BOOKS_API_SECRET"]
        NYT_BOOKS_API_ENDPOINT = configs["API"]["NYT_API_ENDPOINT"]

        logging.info("Successfully parsed configuration file.")
        return NYT_BOOKS_API_KEY, NYT_BOOKS_API_SECRET, NYT_BOOKS_API_ENDPOINT
    except FileNotFoundError:
        logging.error(f"Configuration file {config_file_path} not found.")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error parsing JSON from the configuration file {config_file_path}.")
        raise
    except KeyError as e:
        logging.error(f"Missing expected key in the configuration: {e}")
        raise

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
    logging.info(f"Generating dates from {start_date} to {end_date} with an offset of {offset} days.")

    # Format dates
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    # Init date variable
    date = start_date

    # Init dates list
    dates = []

    # Loop to get all required dates in specified range
    while date <= end_date:
        dates.append(date)
        date += timedelta(days=offset)

    logging.info(f"Generated {len(dates)} dates.")
    return dates



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
    logging.info(f"Fetching data for {DATE} from the NYT Books API.")

    try:
        # Connect to API and pull data
        URL = f"https://api.nytimes.com/svc/books/v3/lists/overview.json?published_date={DATE}&api-key={NYT_BOOKS_API_KEY}"
        response = requests.get(URL)

        if response.status_code == 200:
            # Save the data to a JSON file in the raw_data folder
            os.makedirs("./raw_data", exist_ok=True)

            file_path = f"./raw_data/{DATE}.json"
            with open(file_path, "w") as raw_file:
                json.dump(response.json(), raw_file)

            logging.info(f"Successfully fetched and saved data for {DATE}.")
        else:
            logging.error(f"Failed to fetch data for {DATE}. Status code: {response.status_code}")
            raise Exception(f"Failed to fetch data. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error occurred while making the API request: {e}")
        raise



def init_db_connection(host, database, user, password, port):
    """
    Establishes a connection to a database and returns a cursor object.

    Args:
        host (str): The hostname or IP address of the PostgreSQL server.
        database (str): The name of the database to connect to.
        user (str): The username used to authenticate with the database.
        password (str): The password for the provided username.
        port (int): The port number on which the PostgreSQL server is listening (default is 5432).

    Returns:
        cursor: A psycopg2 cursor object that can be used to interact with the database.
    
    Raises:
        psycopg2.OperationalError: If there is an issue with connecting to the database.
    """
    logging.info(f"Initializing database connection to {host}:{port}/{database}")

    try:
        # Connect to database
        conn = psycopg2.connect(
            host=host,
            dbname=database,
            user=user,
            password=password,
            port=port
        )
        cursor = conn.cursor()

        logging.info("Successfully connected to the database.")
        return conn, cursor
    except psycopg2.OperationalError as e:
        logging.error(f"Error connecting to database: {e}")
        raise



def read_json_file(file_path):
    """
    Reads a JSON file and returns the data as a Python object.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict or list: The parsed JSON data (depends on the structure of the JSON file).
    """
    logging.info(f"Reading JSON file from {file_path}")
    
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)  # Parses the JSON data into a Python object (dict or list)

        logging.info("Successfully read and parsed the JSON file.")
        return data
    except FileNotFoundError:
        logging.error(f"Error: The file {file_path} was not found.")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error: Failed to decode JSON from {file_path}. The file may be malformed.")
        return None



def transform_data(data):
    # Init lists to store each table data
    lists_data = []
    books_data = []
    buy_links_data = []
    fact_best_sellers_data = []

    # Create a list to track rejected records
    rejected_records = []

    # Stage lists
    for list_entry in data['results']['lists']:
        try:
            list_dict = {
                'id': list_entry['list_id'],
                'list_name': list_entry['list_name'],
                'list_name_encoded': list_entry['list_name_encoded'],
                'display_name': list_entry['display_name'],
                'updated': datetime.strptime(data['results']['bestsellers_date'], "%Y-%m-%d").date(),
                'list_image': list_entry.get('list_image', ''),
                'list_image_width': list_entry.get('list_image_width', None),
                'list_image_height': list_entry.get('list_image_height', None),
            }
            lists_data.append(list_dict)
            logger.info(f"Processed list: {list_entry['list_name']}")
        except Exception as e:
            rejected_records.append({
                'error': str(e),
                'record': list_entry,
                'table': 'lists'
            })
            logger.error(f"Error processing list: {list_entry['list_name']} - Error: {e}")

        # Stage books
        for book in list_entry['books']:
            try:
                book_id = book['primary_isbn13']
                book_dict = {
                    'id': book_id,
                    'title': book['title'],
                    'publisher': book['publisher'],
                    'author': book['author'],
                    'contributor': book['contributor'],
                    'contributor_note': book['contributor_note'],
                    'description': book['description'],
                    'created_date': datetime.strptime(book['created_date'], "%Y-%m-%d %H:%M:%S").date(),
                    'updated_date': datetime.strptime(book['updated_date'], "%Y-%m-%d %H:%M:%S").date(),
                    'age_group': book['age_group'],
                    'amazon_product_url': book['amazon_product_url'],
                    'primary_isbn13': book['primary_isbn13'],
                    'primary_isbn10': book['primary_isbn10'],
                    'book_image_width': book['book_image_width'],
                    'book_image_height': book['book_image_height'],
                    'first_chapter_link': book['first_chapter_link'],
                    'book_uri': book['book_uri'],
                    'sunday_review_link': book['sunday_review_link']
                }
                books_data.append(book_dict)
                logger.info(f"Processed book: {book['title']} (ISBN13: {book['primary_isbn13']})")

                # Stage book_buy_links
                for buy_link in book['buy_links']:
                    try:
                        buy_link_dict = {
                            'book_id': book_id,
                            'website_name': buy_link['name'],
                            'website_url': buy_link['url']
                        }
                        buy_links_data.append(buy_link_dict)
                        logger.info(f"Processed buy link for book {book['title']} - {buy_link['name']}")
                    except Exception as e:
                        rejected_records.append({
                            'error': str(e),
                            'record': buy_link,
                            'table': 'books_buy_links'
                        })
                        logger.error(f"Error processing buy link for book {book['title']} - Error: {e}")

            except Exception as e:
                rejected_records.append({
                    'error': str(e),
                    'record': book,
                    'table': 'books'
                })
                logger.error(f"Error processing book {book['title']} - Error: {e}")

            # Stage best_sellers_publish
            try:
                fct_best_seller_dict = {
                    'bestsellers_date': datetime.strptime(data['results']['bestsellers_date'], "%Y-%m-%d").date(),
                    'published_date': datetime.strptime(data['results']['published_date'], "%Y-%m-%d").date(),
                    'previous_published_date': datetime.strptime(data['results']['previous_published_date'], "%Y-%m-%d").date(),
                    'next_published_date': datetime.strptime(data['results']['next_published_date'], "%Y-%m-%d").date(),
                    'list_id': list_entry['list_id'],
                    'book_id': book_id,
                    'rank': book['rank'],
                    'weeks_on_list': book['weeks_on_list'],
                    'price': float(book['price'])
                }
                fact_best_sellers_data.append(fct_best_seller_dict)
                logger.info(f"Processed best seller data for book {book['title']}")
            except Exception as e:
                rejected_records.append({
                    'error': str(e),
                    'record': book,
                    'table': 'best_sellers_publish'
                })
                logger.error(f"Error processing best seller data for book {book['title']} - Error: {e}")

    # Convert to DataFrames
    df_lists = pd.DataFrame(lists_data)
    df_books = pd.DataFrame(books_data)
    df_buy_links = pd.DataFrame(buy_links_data)
    df_best_sellers = pd.DataFrame(fact_best_sellers_data)

    return df_lists, df_books, df_buy_links, df_best_sellers, rejected_records



def load_data(conn, cursor, df_lists, df_books, df_buy_links, df_best_sellers):
    # Create a list to track rejected records
    rejected_records = []
    try:
        # Insert data into DimLists
        execute_batch(cursor, """
            INSERT INTO stage.lists (id, list_name, list_name_encoded, display_name, updated, list_image, list_image_width, list_image_height)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, df_lists.values.tolist())
        logger.info(f"Inserted {len(df_lists)} records into stage.lists")

        # Insert data into DimBooks
        execute_batch(cursor, """
            INSERT INTO stage.books (id, title, publisher, author, contributor, contributor_note, description, created_date, updated_date, age_group, amazon_product_url, primary_isbn13, primary_isbn10, book_image_width, book_image_height, first_chapter_link, book_uri, sunday_review_link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, df_books.values.tolist())
        logger.info(f"Inserted {len(df_books)} records into stage.books")

        # Insert data into DimBooksBuyLinks
        execute_batch(cursor, """
            INSERT INTO stage.books_buy_links (book_id, website_name, website_url)
            VALUES (%s, %s, %s)
        """, df_buy_links.values.tolist())
        logger.info(f"Inserted {len(df_buy_links)} records into stage.books_buy_links")

        # Insert data into best_sellings_lists_books
        execute_batch(cursor, """
            INSERT INTO stage.best_sellings_lists_books (bestsellers_date, published_date, previous_published_date, next_published_date, list_id, book_id, rank, weeks_on_list, price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, df_best_sellers.values.tolist())
        logger.info(f"Inserted {len(df_best_sellers)} records into stage.best_sellers_puplish")

        # Commit the transaction
        conn.commit()
        logger.info("Data loaded successfully into the database.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error occurred during data load: {str(e)}")

    finally:
        # Log rejected records if any
        if rejected_records:
            logger.error(f"Rejected records: {rejected_records}")
        else:
            logger.info("No rejected records.")



def write_rejected_records_to_file(rejected_data, file_path="rejected_records.csv"):
    """
    Writes rejected records to a CSV file and logs the process.

    Args:
    rejected_data (list of dicts): List of rejected records containing error information.
    file_path (str): Path to the CSV file where rejected records will be stored.
    """
    try:
        # Add timestamp to each rejected record
        for record in rejected_data:
            record['run_ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Convert rejected_data list of dicts into a DataFrame
        df_rejected = pd.DataFrame(rejected_data)

        # Check if DataFrame is empty before writing
        if not df_rejected.empty:
            # Write rejected records to a CSV file (or change to your desired storage)
            df_rejected.to_csv(file_path, mode='a', header=False, index=False)
            logging.info(f"Successfully wrote {len(rejected_data)} rejected records to {file_path}.")
        else:
            logging.warning("No rejected records to write.")
    
    except Exception as e:
        logging.error(f"Error writing rejected records: {e}")
        raise



def validate_published_dates(cursor, expected_min_date, expected_max_date):
    """
    Validates the minimum and maximum published_date from the database against expected values.

    Args:
    cursor: cursor object to a database.
    expected_min_date (str): Expected minimum published_date in 'YYYY-MM-DD' format.
    expected_max_date (str): Expected maximum published_date in 'YYYY-MM-DD' format.

    Returns:
    bool: True if the dates are valid, otherwise False.
    """
    
    try:
        # Execute the SQL query to fetch min and max published_date
        cursor.execute("""
            SELECT MIN(published_date), MAX(published_date)
            FROM stage.best_sellings_lists_books
        """)
        
        # Fetch the result
        min_date, max_date = cursor.fetchone()

        # Log the fetched dates
        logging.info(f"Fetched Min Date: {min_date}, Max Date: {max_date}")

        # Convert the result to string (if needed)
        min_date = min_date.strftime('%Y-%m-%d') if min_date else None
        max_date = max_date.strftime('%Y-%m-%d') if max_date else None

        # Validate the min and max dates against expected values
        if min_date != expected_min_date:
            logging.error(f"Min Date mismatch: Expected {expected_min_date}, but got {min_date}")
            return False
        
        if max_date != expected_max_date:
            logging.error(f"Max Date mismatch: Expected {expected_max_date}, but got {max_date}")
            return False
        
        logging.info(f"Validation successful: Min Date = {min_date}, Max Date = {max_date}")
        return True
    
    except Exception as e:
        logging.error(f"Error during date validation: {e}")
        return False
    
    finally:
        cursor.close()