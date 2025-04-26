# Sparkify Data Warehouse Project
## Project Overview
Sparkify, a music streaming startup, has significant growth in their user base and song database. To support deeper user behavior analytics, Sparkify wants to move their data infrastructure to the cloud. 
This database provides a centralized, scalable, and structured environment for Sparkify's analytical workloads. With increasing volumne and complexity of data, traditional processing methods were becoming less effecient. Implementing a cloud-based ETL pipeline and dimensional model in Redshift allows Sparkify to find insights into what songs their users are listening to much easier and more efficiently.
## Table of Content

- [Database Schema](#database-schema)
- [ETL Pipeline](#etl-pipeline)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Sample Queries](#sample-queries)

## Database Schema
This database uses a star schema with the following tables:

**Fact Table**

`songplays` - records in event data associated with song plays

- **songplay_id** INTEGER
- **start_time** TIMESTAMP
- **user_id** INTEGER
- **level** VARCHAR(4)
- **song_id** VARCHAR(18)
- **artist_id** VARCHAR(18)
- **session_id** INTEGER
- **location** VARCHAR(65535)
- **user_agent** VARCHAR(65535)

**Dimension Tables**

`users` - users in the app

- **user_id** INTEGER
- **first_name** VARCHAR
- **last_name** VARCHAR
- **gender** VARCHAR
- **level** VARCHAR(4)

`songs` - songs in music database

- **song_id** VARCHAR(18)
- **title** VARCHAR(65535)
- **artist_id** VARCHAR(18)
- **year** INTEGER
- **duration** FLOAT

`artists` - artists in music database

- **artist_id** VARCHAR(18)
- **name** VARCHAR(65535)
    - Some `artist_name` exceed the default maximum length (256) of `VARCHAR` in Redshift
- **location** VARCHAR(65535)
- **latitude** FLOAT
- **longitude** FLOAT

`time` - timestamps of records in **songplays** broken down into specific units

- **start_time** TIMESTAMP
- **hour** INTEGER
- **day** INTEGER
- **week** INTEGER
- **month** INTEGER
- **year** INTEGER
- **weekday** VARCHAR(9)

This schema supports complex queries and aggregations like:

- Most played artists by user
- User retention and subscription level trends
- Total play time on specific days by user

## ETL Pipeline

**1. Create Tables:**

- Create staging tables for storing raw data from JSON files - `staging_events` and `staging_songs`
- Avoid `NOT NULL` constraints for these tables to capture all the raw data
- Create the fact and dimension tables with appropriate keys and constraints for analysis

**2. Load Data from S3 to Redshift:**

- `COPY` raw JSON logs stored in S3 buckets to the staging tables

**3. Transform and Load:**

- Populate the fact and dimension tables using SQL `INSERT` statements
- Duplicates and null values are handled to ensure data integrity

## Project Structure
```
project-root/
├── create_tables.py       # create tables in database
├── dwh.cfg                # config file
├── etl.py                 # perform the ETL pipeline
├── sql_queries.py         # contains all SQL queries used in the project
├── sample_queries.py      # queries row count for all tables and print output
└── README.md              # Project documentation
```
## Usage
1. Make sure you have a Redshift cluster available and an IAM Role associated with it. If not, create one.
2. Add Redshift database and IAM Role info to `dwh.cfg`.

    Note: `HOST` means the `Endpoint` without the port number (everything before the `:`)

3. Run `create_tables.py` to create tables in the Redshift database.
4. (Optional) Use Query Editor in the AWS Redshift console to check if the tables are properly created.
5. Run `etl.py` to load data from S3 buckets to Redshift database.
## Sample Queries
Run `sample_queries.py` to get row counts for all tables.

**Output:**
```
Table: time, Row Count: 8023
Table: artists, Row Count: 45237
Table: songs, Row Count: 383950
Table: users, Row Count: 105
Table: songplays, Row Count: 6627
Table: staging_events, Row Count: 8056
Table: staging_songs, Row Count: 383950
```
