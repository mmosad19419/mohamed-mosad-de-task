## Senior Data Engineer Task

# NYT Best Sellers Data Analysis

## Overview

This project analyzes the New York Times (NYT) Best Sellers data to determine which book stayed in the top 3 ranks for the longest time in 2022. The dataset is fetched from the NYT Books API and then processed to extract key insights, including identifying the book with the longest duration in the top 3 positions. Additionally, the project handles the rejection of invalid records and writes these rejected records to a specific directory for further review.

### Objective:
You will be working with the New York Times Books API, particularly the [lists overview endpoint](https://developer.nytimes.com/docs/books-product/1/routes/lists/overview.json/get). In case of any questions with the API, check out their [FAQ](https://developer.nytimes.com/faq).


## Questions to Answer
- Which book remained in the top 3 ranks for the longest time in 2022.<br>
- Which are the top 3 lists to have the least number of unique books in their
    rankings for the entirety of the data<br>
- Publishers are ranked based on how their respective books performed on this
    list. For each book, a publisher gets points based on the best rank a book got in a
given period of time. The publisher gets 5 points if the book is ranked 1st, 4 for
2nd rank, 3 for 3rd rank, 2 for 4th and 1 point for 5th. Create a quarterly rank for
publishers from 2021 to 2023, getting only the top 5 for each quarter.<br>
- Two friends Jake and Pete have podcasts where they review books. Jake's team
reviews the book ranked first on every list, while Pete’s team reviews the book
ranked third. Both of them share books, if Jake’s team wants to review a book,
they first check with Pete’s before buying and vice versa. Which team bought
what book in 2023?

## Features

- **Data Fetching**: Retrieves data from the NYT Books API for a given date range (in this case, 2021-2023).
- **Data Processing**: preprocess the data
- **Dimensional Data Modeling**: Model the data we get from the API after preprocess to derive insights.
- **Data Ingestion**: Ingest the Data to Data Warehouse.
- **Rejected Records**: Records that couldn't be processed are logged into a separate folder with additional metadata.
- **Database Integration**: Insert data into relational databases aka PostgreSQL.
- **Unit Testing**: Unit tests for critical functions such as data fetching, processing, and error handling.

## Prerequisites

### Python Version

- Python 3.x is required.


# NYT Books API - Lists Overview Endpoint

The **lists/overview** endpoint retrieves the New York Times Best Sellers lists for a specific date. If the date specified doesn't exactly match a published list, the service will find the closest available list for that date.

## HTTP Request GET https://api.nytimes.com/svc/books/v3/lists/overview.json


## Query Parameters

### `published_date` (Optional)

- **Type**: `string`
- **Format**: `YYYY-MM-DD`
- **Pattern**: `^\d{4}-\d{2}-\d{2}$`
- **Description**: 

    The **published_date** specifies the date for which you want to retrieve the Best Sellers list. 

    - If a valid date is provided, the endpoint will retrieve the list for that specific date.
    - If the provided date does not have a corresponding list, the API will return the closest available list for that date.
    - If you do not include the `published_date` parameter, the API will return the **current week's** Best Sellers list.

### Example

- A request for `lists/overview/2013-05-22` will retrieve the Best Sellers list published closest to **May 22, 2013**.
- A request without the `published_date` parameter will return the list for the **current week**.

## Example Request
GET https://api.nytimes.com/svc/books/v3/lists/overview.json?published_date=2023-01-01&api-key=your_api_key


### Example Response

```json
{
  "status": "OK",
  "results": {
    "lists": [
      {
        "list_id": 1,
        "list_name": "Hardcover Fiction",
        "display_name": "Hardcover Fiction",
        "updated": "2023-01-01",
        "books": [
          {
            "title": "The Midnight Library",
            "author": "Matt Haig",
            "rank": 1,
            "weeks_on_list": 10,
            "primary_isbn13": "9780525559474",
            "primary_isbn10": "0525559477"
          },
          {
            "title": "The Four Winds",
            "author": "Kristin Hannah",
            "rank": 2,
            "weeks_on_list": 8,
            "primary_isbn13": "9781250178602",
            "primary_isbn10": "1250178606"
          }
        ]
      }
    ]
  }
}
```

## Dimensional Data Model
To understand the relationships between the various entities in the database, here's an Entity Relationship Diagram (ERD):

![Dimensional Model](https://github.com/user-attachments/assets/36b49206-377a-477d-b0cc-583eff90da2e)


# DBT Project: Best Seller Book Lists

This repository contains a DBT (Data Build Tool) project that processes and transforms raw data about best-seller books, including book details, sales rankings, and metadata, into meaningful insights.

The project includes models that ingest raw data (Bronze layer), transform the data (Silver layer), and prepare it for business consumption (Gold layer).

---

## Prerequisites

Before getting started, ensure that you have the following installed:

- **Python** (>= 3.7)
- **DBT** (>= 1.0)
- A **Database** connection (Postgres)

---


### Explanation of Steps:

- **Step 1**: Sets up a virtual environment for isolated Python dependencies.
- **Step 2**: Starts the required services (PostgreSQL, etc.) using Docker.
- **Step 3**: Configures the NYT Books API Key in the `config.json` file for accessing data.
- **Step 4-5**: Accesses PGAdmin to connect and manage the PostgreSQL database.
- **Step 6**: Creates necessary schema in the database.
- **Step 7**: Fetches and ingests the raw data from the NYT Books API into the database.
- **Step 8**: Runs DBT to transform and process the raw data.
- **Step 9**: Queries the results from the database.
- **Step 10**: Exports the final results to a CSV for further analysis.

This markdown file organizes the entire workflow into easy-to-follow steps with clear instructions.

# Project Walkthrough

Follow these steps to rerun and set up the project:

## 1. Set up Virtual Environment

First, create a Python virtual environment to isolate project dependencies:

```bash
python -m venv .venv
```

Activate the virtual environment:

```bash
source .venv/bin/activate
.venv\Scripts\activate
```

Install the required dependencies from the `requirements.txt` file:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## 2. Set up Docker

Start the Docker containers using Docker Compose:

```bash
docker compose up -d
```

This will start all required services such as the PostgreSQL database.

## 3. Update API Key

- Open the `config.json` file located in the root directory and airflow directory `airflow/config/config.json`.
- Update the **NYT Books API Key** with your actual API key, which you can obtain from the [NYT Developer Portal](https://developer.nytimes.com/).

```json
{
  "API": {
    "NYT_BOOKS_API_KEY": "your_api_key_here",
    "NYT_BOOKS_API_SECRET": "your_api_secret_here",
    "NYT_API_ENDPOINT": "https://api.nytimes.com/svc/books/v3/lists/overview.json"
  }
}
```

## 4. Access PGAdmin

- Open your browser and go to the following address:

```
http://localhost:80
```

- Log in with the following credentials:

  - **Email**: `admin@admin.com`
  - **Password**: `admin123`

## 5. Register PostgreSQL Server in PGAdmin

- **Host**: `postgres`
- **User**: `admin`
- **Password**: `admin`
- **Port**: `5432`

Register the server using these credentials.

## 6. Create Stage Layer

Once you have logged into PGAdmin, execute the `sql/create_stage_layer.sql` file to set up the required database schema.

## 7. Run Data Ingestion Scripts
- Move to airflow directory:
```bash
cd airflow
```
- Setup AIRFLOW_HOME directory

```bash
export DBT_PROFILES_DIR=$(pwd)
```

- Run unit tests:
```bash
pytest
```

- run airflow:
```bash
airflow standalone
```

###  Run the data pulling script to fetch data from the NYT Books API:
1. Using Airflow Dag
- go to the `http://localhost:8080/`
- sign in with user: admin, passward provided in `standalone_admin_password.txt` file
- trigger the `nyt_books_etl_pipeline` dag

2. Manually
**Run**
- Navigate to the root directory
```bash
cd ..
```

- Run the following commands
```bash
python src/pulling_lists_reviews_data.py
```

- Ingest the raw data into the database’s stage layer:

```bash
python src/ingest_raw_data_to_stage.py
```

## 8. Set Up DBT (Data Build Tool)

- Navigate to the `nyt_lists_reviews/` directory:
```bash
cd nyt_lists_reviews/
```

- Print the working directory to confirm the path:
Export the `DBT_PROFILES_DIR` environment variable, pointing to your dbt directory current directory
```bash
export DBT_PROFILES_DIR=$(pwd)
```

## `~/.dbt/profiles.yml`

The `profiles.yml` file is used by DBT to configure database connections. Below is an example configuration for connecting to a PostgreSQL database:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost       # Replace with your PostgreSQL host 
      user: admin           # Replace with your PostgreSQL username
      password: admin       # Replace with your PostgreSQL password
      port: 5431            # Replace with the port used to listen to request to db
      dbname: mydb          # Replace with the name of your database
      schema: dwh           # Replace with the schema name where DBT models will be stored
      threads: 1            # Number of threads DBT will use for parallel execution
```


- Run DBT in debug mode to ensure everything is configured properly:

```bash
dbt debug
```

- Run DBT to build the models and refresh the data:

```bash
dbt run --full-refresh
```

## 9. Generate Final Results

After DBT finishes running, go back to PGAdmin and execute the `sql/results.sql` query to fetch the results of the analysis.

## 10. Export Results

Once the results are displayed in PGAdmin, you can export them to a **CSV** file:

1. Right-click on the results table.
2. Select **Export Data**.
3. Choose **CSV** format and specify the location to save the file.

## 11- Tear down all resources
 ```bash
  docker compose down
  ```
---

### Congratulations! 🎉

You have successfully completed the process of setting up, running, and exporting the results for the NYT Best Sellers Data Analysis project.


## Enhancements
- CI CD pipeline
- Encapsulate the process in Airflow Dag
- Enhance Code Quality
- More unit tests
- More Data quality checks
- Data validation checks when inserting the data to staging tables
- Enhance the Dimensional Model handling SCD
- Add uniquly generated key for each table
- Better Documentation