import requests
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch

from utils.helper_functions import  init_db_connection, generate_incremental_dates, read_json_file, transform_data, load_data, write_rejected_records_to_file, validate_published_dates

conn, cursor = init_db_connection("localhost", "mydb", "admin", "admin", "5432")

START_DATE = '2020-12-27'
END_DATE = '2023-12-31'
OFFSET = 7
dates = generate_incremental_dates(START_DATE, END_DATE, OFFSET)

for date in dates:
    data = read_json_file(f'./raw_data/{date}.json')
    df_lists, df_books, df_buy_links, df_best_sellers, rejected_records = transform_data(data)
    load_data(conn, cursor, df_lists, df_books, df_buy_links, df_best_sellers)\

cursor.close()
conn.close()

write_rejected_records_to_file(rejected_records)

validate_published_dates(cursor, START_DATE, END_DATE)