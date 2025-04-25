import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data to tables listed in the `copy_table_queries` list.
    
    Args:
        cur: Cursor object to execute PostgreSQL commands.
        conn: Connection to the database.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data to tables listed in the `insert_table_queries` list.
    
    Args:
        cur: Cursor object to execute PostgreSQL commands.
        conn: Connection to the database.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that orchestrates the following steps:
    1. Reads configuration from `dwh.cfg`.
    2. Connects to the Redshift database.
    3. Load data to staging tables.
    4. Insert data to fact and dimension tables.
    5. Closes the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    try:
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
    except Exception as e:
        print(f"Error: {e}")

    conn.close()


if __name__ == "__main__":
    main()