import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables listed in the `drop_table_queries` list.

    Args:
        cur: Cursor object to execute PostgreSQL commands.
        conn: Connection to the database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables listed in the `create_table_queries` list.
    
    Args:
        cur: Cursor object to execute PostgreSQL commands.
        conn: Connection to the database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that orchestrates the following steps:
    1. Reads configuration from `dwh.cfg`.
    2. Connects to the Redshift database.
    3. Drops existing tables.
    4. Creates new tables.
    5. Closes the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()