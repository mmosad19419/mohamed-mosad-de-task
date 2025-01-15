import requests
import pandas as pd
from datetime import datetime
import psycopg
# from psycopg2.extras import execute_batch


def transform_data(data):
    # init lists to store each table data
    lists_data = []
    books_data = []
    buy_links_data = []
    fact_best_sellers_data = []
    date_data = []

    # DimLists
    for list_entry in data['results']['lists']:
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

        # DimBooks
        for book in list_entry['books']:
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

            # DimBooksBuyLinks
            for buy_link in book['buy_links']:
                buy_link_dict = {
                    'book_id': book_id,
                    'website_name': buy_link['name'],
                    'website_url': buy_link['url']
                }
                buy_links_data.append(buy_link_dict)

            # Fct_best_sellers_publish
            fct_best_seller_dict = {
                'bestsellers_date': datetime.strptime(data['results']['bestsellers_date'], "%Y-%m-%d").date(),
                'published_date': datetime.strptime(book['published_date'], "%Y-%m-%d").date(),
                'previous_published_date': datetime.strptime(data['results']['previous_published_date'], "%Y-%m-%d").date(),
                'next_published_date': datetime.strptime(data['results']['next_published_date'], "%Y-%m-%d").date(),
                'list_id': list_entry['list_id'],
                'book_id': book_id,
                'rank': book['rank'],
                'weeks_on_list': book['weeks_on_list'],
                'price': book['price']
            }
            fact_best_sellers_data.append(fct_best_seller_dict)


    # Convert to DataFrames
    df_lists = pd.DataFrame(lists_data)
    df_books = pd.DataFrame(books_data)
    df_buy_links = pd.DataFrame(buy_links_data)
    df_best_sellers = pd.DataFrame(fact_best_sellers_data)

    return df_lists, df_books, df_buy_links, df_best_sellers, df_dates


# Function to load the transformed data into the DWH
def load_data(df_lists, df_books, df_buy_links, df_best_sellers, df_dates):
    # Insert data into DimLists
    execute_batch(cursor, """
        INSERT INTO DimLists (id, list_name, list_name_encoded, display_name, updated, list_image, list_image_width, list_image_height)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, df_lists.values.tolist())

    # Insert data into DimBooks
    execute_batch(cursor, """
        INSERT INTO DimBooks (id, title, publisher, author, contributor, contributor_note, description, created_date, updated_date, age_group, amazon_product_url, primary_isbn13, primary_isbn10, book_image_width, book_image_height, first_chapter_link, book_uri, sunday_review_link)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, df_books.values.tolist())

    # Insert data into DimBooksBuyLinks
    execute_batch(cursor, """
        INSERT INTO DimBooksBuyLinks (book_id, website_name, website_url)
        VALUES (%s, %s, %s)
    """, df_buy_links.values.tolist())

    # Insert data into Fct_best_sellers_publish
    execute_batch(cursor, """
        INSERT INTO Fct_best_sellers_puplish (bestsellers_date, published_date, previous_published_date, next_published_date, list_id, book_id, rank, weeks_on_list, price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, df_best_sellers.values.tolist())

     # Insert data into DimDate
    execute_batch(cursor, """
        INSERT INTO DimDate (DateKey, FullDate, DayOfMonth, DayName, DayOfWeek, DayOfYear, WeekOfYear, Month, MonthName, Quarter, Year, MonthYear)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, df_dates.values.tolist())

    # Commit and close
    conn.commit()
    cursor.close()
    conn.close()

# ETL Pipeline
if __name__ == '__main__':
        df_lists, df_books, df_buy_links, df_best_sellers, df_dates = transform_data(data)
        load_data(df_lists, df_books, df_buy_links, df_best_sellers, df_dates)