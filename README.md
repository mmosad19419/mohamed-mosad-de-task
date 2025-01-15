## Senior Data Engineer Task

# NYT Best Sellers Data Analysis

## Overview

This project analyzes the New York Times (NYT) Best Sellers data to determine which book stayed in the top 3 ranks for the longest time in 2022. The dataset is fetched from the NYT Books API and then processed to extract key insights, including identifying the book with the longest duration in the top 3 positions. Additionally, the project handles the rejection of invalid records and writes these rejected records to a specific directory for further review.

## Questions to Answer
● Which book remained in the top 3 ranks for the longest time in 2022.<br>
● Which are the top 3 lists to have the least number of unique books in their
    rankings for the entirety of the data<br>
● Publishers are ranked based on how their respective books performed on this
    list. For each book, a publisher gets points based on the best rank a book got in a
given period of time. The publisher gets 5 points if the book is ranked 1st, 4 for
2nd rank, 3 for 3rd rank, 2 for 4th and 1 point for 5th. Create a quarterly rank for
publishers from 2021 to 2023, getting only the top 5 for each quarter.<br>
● Two friends Jake and Pete have podcasts where they review books. Jake's team
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

### Required Python Libraries

To create virtual env and install the required libraries, use the following commands:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

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


### Objective:
You will be working with the New York Times Books API, particularly the [lists overview endpoint](https://developer.nytimes.com/docs/books-product/1/routes/lists/overview.json/get). In case of any questions with the API, check out their [FAQ](https://developer.nytimes.com/faq).


## Dimensional Data Model
To understand the relationships between the various entities in the database, here's an Entity Relationship Diagram (ERD):

![Dimensional Model](https://github.com/user-attachments/assets/36b49206-377a-477d-b0cc-583eff90da2e)


# Enhancements
- Add uniquly generated key for each table
- More unit tests
- Data quality checks
- Data validation checks when inserting the data to staging tables
- CI / CD pipleine
- Better Documentation


# DBT Project: Best Seller Book Lists

This repository contains a DBT (Data Build Tool) project that processes and transforms raw data about best-seller books, including book details, sales rankings, and metadata, into meaningful insights.

The project includes models that ingest raw data (Bronze layer), transform the data (Silver layer), and prepare it for business consumption (Gold layer).

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup Steps](#setup-steps)
  - [Install DBT](#install-dbt)
  - [Configure DBT Profile](#configure-dbt-profile)
  - [Install Dependencies](#install-dependencies)
- [DBT Environment Setup](#dbt-environment-setup)
  - [Setting up DBT for Different Environments](#setting-up-dbt-for-different-environments)
  - [Switching Between Environments](#switching-between-environments)
- [Running the Project](#running-the-project)
  - [Running All Models](#running-all-models)
  - [Running a Specific Model](#running-a-specific-model)
  - [Using `--full-refresh`](#using-full-refresh)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## Prerequisites

Before getting started, ensure that you have the following installed:

- **Python** (>= 3.7)
- **DBT** (>= 1.0)
- A **Database** connection (Postgres)

---

## Project Structure

The DBT project is structured as follows:

dbt_project/ 
│ 
├── models/ # DBT models (Bronze, Silver, Gold layers) 
│        │ 
│        ├── bronze/ # Raw data layer (staging) 
│        │ 
│        ├── silver/ # Transformed data layer 
│        │ 
│        └── gold/ # Business-ready data layer 
│ 
├── macros/
├── snapshots/
├── analysis/
├── dbt_project.yml
├── profiles.yml 
└── README.md 


---

## Setup Steps

Follow these steps to set up the DBT project on your local machine or a different environment.

### Install DBT

1. **Install DBT using pip**:
   Run the following command to install DBT globally on your system:

   ```bash
   pip install dbt

~/.dbt/profiles.yml


my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: your-database-host
      user: your-database-user
      password: your-database-password
      dbname: your-database-name
      schema: your-schema-name
      threads: 4
## Adjust the above details according to your database provider.

pip install -r requirements.txt

dbt run --full-refresh
