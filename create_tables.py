import configparser                     # Parse configuration file
from logging import error
import psycopg2                         # Connect to Redshift using PostgreSQL adapter for Python
import sys                              
# SQL query definitions
from sql_queries import drop_table_queries, create_table_queries    

"""
Purpose:
  - Execute DROP TABLE queries listed in drop_table_queries
  - drop_table_queries is defined in sql_queries file
Param:
  - @cur: Redshift connection cursor
  - @conn: Redshift connection
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
Purpose:
  - Execute CREATE TABLE queries listed in create_table_queries
  - create_table_queries is defined in sql_queries file
Param:
  - @cur: Redshift connection cursor
  - @conn: Redshift connection
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

"""
Purpose:
  - Read Redshift cluster connection details from dwh.cfg config file
  - Connect to cluster using config details and retrieve Redshift Connection and Cursor handle
  - Call drop_tables() and create_table() functions
"""
def main():
    config = configparser.ConfigParser()
    # Open and read config file to retrieve Redshift and DWH details required to connect
    try:
        config.read('dwh.cfg')
    except Exception as e:
        error("Error reading Config file: ", e)
        sys.exit()

    # Connect using cluster and DWH details
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    except Exception as e:
        error("Error connecting Data Warehouse: ", e)
        sys.exit()

    # Get conection cursor
    try:
        cur = conn.cursor()
    except Exception as e:
        error("Error getting connection cursor: ", e)
        conn.close()
        sys.exit()

    # Call DROP TABLE queries
    try:
        drop_tables(cur, conn)
    except Exception as e:
        error("Error dropping tables: ", e)
        conn.close()
        sys.exit()

    # Call CREATE TABLE queries
    try:
        create_tables(cur, conn)
    except Exception as e:
        error("Error creating tables: ", e)
        conn.close()
        sys.exit()

    # Closing connection
    conn.close()

"""
  - Run above code if the file is labled __main__
  - Python internally labels files at runtime to differentiate between imported files and main file
"""
if __name__ == "__main__":
    main()