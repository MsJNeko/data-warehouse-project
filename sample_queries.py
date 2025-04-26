import configparser
import psycopg2

def get_table_row_count(cur, conn):
    """
    Get the row count for each table in the database.
    
    Args:
        cur: Cursor object to execute PostgreSQL commands.
        conn: Connection to the database.
        
    Returns:
        A dictionary with table names as keys and their respective row counts as values.
    """
    table_row_count = {}
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    cur.execute(query)
    tables = cur.fetchall()
    
    for table in tables:
        table_name = table[0]
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        cur.execute(count_query)
        count = cur.fetchone()[0]
        table_row_count[table_name] = count
    
    return table_row_count  

def main():
    """
    Main function that orchestrates the following steps:
    1. Reads configuration from `dwh.cfg`.
    2. Connects to the Redshift database.
    3. Get row count for all tables in the database.
    4. Print the table names and their corresponding row counts.
    5. Closes the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    try:
        table_row_count = get_table_row_count(cur, conn)
        for table, count in table_row_count.items():
            print(f"Table: {table}, Row Count: {count}")
    except Exception as e:
        print(f"Error: {e}")

    conn.close()


if __name__ == "__main__":
    main()