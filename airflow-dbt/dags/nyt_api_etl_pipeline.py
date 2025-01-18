import logging
from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Import helper functions
from plugins.helper_functions import parse_configs, generate_incremental_dates, fetch_data_from_api, \
    init_db_connection, read_json_file, transform_data, load_data, write_rejected_records_to_file, \
    validate_published_dates

# Fetching the API Credentials
NYT_BOOKS_API_KEY, NYT_BOOKS_API_SECRET, NYT_BOOKS_API_ENDPOINT = parse_configs('./config/config.json')

# Define the start and end dates for the range
START_DATE = '2020-12-27'
END_DATE = '2023-12-31'
OFFSET = 7

# Fetch Data from API
def fetch_data_function():
    # Generate the list of dates for incremental loading
    dates = generate_incremental_dates(START_DATE, END_DATE, OFFSET)

    # make api calls for each date
    for date in dates:
        try:
            logger.info(f"Starting fetch for date {date}")
            fetch_data_from_api(NYT_BOOKS_API_KEY, date)
            logger.info(f"Successfully fetched data for date {date}")
            time.sleep(12)  # Sleep for 12 seconds to avoid hitting API rate limits
        except Exception as e:
            logger.error(f"Error fetching data for date {date}: {e}")
            raise

# Task 2: Process Data
def process_data_function(date):
    # Generate the list of dates for incremental loading
    dates = generate_incremental_dates(START_DATE, END_DATE, OFFSET)

     # process raw data files for each date
    for date in dates:
        try:
            logger.info(f"Starting data processing for {date}")
            # Read the raw data
            data = read_json_file(f'./raw_data/{date}.json')

            # Transform the data
            df_lists, df_books, df_buy_links, df_best_sellers, rejected_records = transform_data(data)

            # Load data into the database
            conn, cursor = init_db_connection("localhost", "mydb", "admin", "admin", "5432")
            load_data(conn, cursor, df_lists, df_books, df_buy_links, df_best_sellers)

            # Handle rejected records
            write_rejected_records_to_file(rejected_records)

            logger.info(f"Successfully processed data for {date}")
        except Exception as e:
            logger.error(f"Error processing data for {date}: {e}")
            raise
    
    # Close the DB connection
    cursor.close()
    conn.close()

# Validate Data ingested date range
def validate_data_task():
    try:
        logger.info("Starting data validation")
        conn, cursor = init_db_connection("localhost", "mydb", "admin", "admin", "5432")
        validate_published_dates(cursor, START_DATE, END_DATE)
        cursor.close()
        conn.close()
        logger.info("Data validation completed successfully")
    except Exception as e:
        logger.error(f"Error during data validation: {e}")
        raise


# Define the DAG
with DAG(
    'nyt_books_etl_pipeline',
    default_args={
        'owner': 'mmosad19419@gmail.com',
        'start_date': days_ago(1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='@weekly',
    catchup=False,
    tags=['etl', 'books', 'nyt']
) as dag:

    # Set up logging
    logger = logging.getLogger('airflow.task')

    fetch_data = validate_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_function
    )


    process_data = validate_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data_function
    )

    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data_task
    )

    # Set up tasks dependiencies
    fetch_data >> process_data
    process_data >> validate_data