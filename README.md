# Hospitals Data Pipeline

This reposotory was authored in collaboration with Daven Lagu and Shiyu Wu at Carnegie Mellon Univerity in the Fall of 2023.

## Overview
This reporitory contains a data management system designed for efficient handling of hospital and healthcare data. It utilizes PostgreSQL for database management and Python for data manipulation and interaction with the database. The project includes scripts for setting up the database, loading data, and cleaning up.

## File Descriptions

### `credentials_template.py`
This template stores database credentials. Users should create a copy of this file, rename it to `credentials.py`, and fill in their database name, username, and password. Credentials are protected from being pushed to the repository by `.gitignore`.

### `table_setup.py`
Sets up database tables in PostgreSQL for storing hospital information, weekly data, quality ratings, and geolocation data.

ex.(python table_setup.py)

### `table_teardown.py`
Deletes tables created by `table_setup.py`, useful for database cleanup or reset.

ex.(python table_teardown.py)

### `load-hhs.py`
Loads weekly hospital data into the database. It processes and inserts data from a specified CSV file, ensuring no duplicate entries.

ex.(python load-hhs.py)

**Usage:**
  ```bash
python load-hhs.py [file-name]
```
Weekly Updates: The script accepts a CSV file (e.g., 2022-01-04-hhs-data.csv) containing weekly data, processes it (e.g., converting -999 to None, parsing dates), and inserts the data into the database.

ex.(python load-hhs.py data/{.csv name})

{.csv name} should always call YYYY-MM-DD-hhs-data.csv for year and month that matches {date}.

### `load-quality.py`
Loads hospital quality data into the database. It takes a date and file name as command-line arguments and updates the database accordingly.

**Usage:**
  ```bash
python load-quality.py [date] [file-name]
```
CMS Quality Data: The script accepts a date (YYYY-mm-dd) and a CSV file name (e.g., Hospital_General_Information-2021-07.csv). It processes and INSERTs the data, including automatic updates for new hospitals in the hospitals table.

ex.(python load-quality.py {date} data/{.csv name})

{date} should be in form "YYYY-MM-DD" where DD should always be 01

{.csv name} should always call Hospital_General_Information-YYYY-MM for year and month that matches {date}.

### `generate_report.py`

This file generates a report on the last 5 weeks of hhs data and gives a summary of the quality data as well. When run, the file outputs an HTML file to the /reports directory with tables and visualizations summarizing the data.

**Usage:**
  ```bash
python generate_report.py [filename].html [date]
```

Ensure that the report file is named something meaningful with the associated date in the file name.

The date should be the end of a selected 4 week period that is being looked at. The date, along with the 4 weeks of data before, will be shown. Format the date like in the following example.

ex.(python generate_report.py hospital_report_2022_10_21.html 2022-10-21)

## Installation

Ensure Python, psycopg2-binary, plotly.express, pandas, and numpy are installed. Set up your PostgreSQL database and update credentials.py with your database details (credentials for username and password should come from Alex's email of credentials for PostgreSQL).

## Usage

1. Update credentials.py.
2. Run table_setup.py to create tables.
3. Use load-hhs.py and load-quality.py to populate tables with data.
4. Optional: Use table_teardown.py to remove tables.
