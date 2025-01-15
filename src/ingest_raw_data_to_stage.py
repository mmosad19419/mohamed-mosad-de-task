import requests
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch

from utils.helper_functions import  init_db_connection, generate_incremental_dates, read_json_file, transform_data, load_data, write_rejected_records_to_file, validate_published_dates

# Init database connection
conn, cursor = init_db_connection("localhost", "mydb", "admin", "admin", "5432")

# generate weekly dates starting from 2021 to 2023
START_DATE = '2020-12-27'
END_DATE = '2023-12-31'
OFFSET = 7
dates = generate_incremental_dates(START_DATE, END_DATE, OFFSET)

# preprocess and ingest the data 
for date in dates:
    data = read_json_file(f'./raw_data/{date}.json')
    df_lists, df_books, df_buy_links, df_best_sellers, rejected_records = transform_data(data)
    load_data(conn, cursor, df_lists, df_books, df_buy_links, df_best_sellers)\

# handling rejected records
write_rejected_records_to_file(rejected_records)

# do simple validation that the data ingested for the while range
validate_published_dates(cursor, START_DATE, END_DATE)

# close database connection
cursor.close()
conn.close()