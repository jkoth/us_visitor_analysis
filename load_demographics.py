import configparser                    # Parse configuration file
from logging import error
import psycopg2                        # Connect to Redshift using PostgreSQL adapter for Python
import sys                                              
# SQL query definitions
from sql_queries import copy_demo_table_queries, trunc_demo_table_queries
from sql_queries import check_unique_key, check_zero_count

"""
Purpose:
  - Execute COPY queries listed in copy_demo_table_queries
  - copy_demo_table_queries is defined in sql_queries file
Param:
  - @cur: Redshift connection cursor
  - @conn: Redshift connection
"""
def copy_tables(cur, conn):
    for query in copy_demo_table_queries:
        cur.execute(query)
        conn.commit()

"""
Purpose:
  - Execute TRUNCATE queries listed in trunc_demo_table_queries
  - trunc_demo_table_queries are defined in sql_queries file
Param:
  - @cur: Redshift connection cursor
  - @conn: Redshift connection
"""
def trunc_tables(cur, conn):
    for query in trunc_demo_table_queries:
        cur.execute(query)
        conn.commit()

"""
Purpose:
  - Read Redshift cluster connection details from dwh.cfg config file
  - Connect to cluster using config details and retrieve Redshift Connection and Cursor handle
  - Call copy_tables() and insert_tables() functions
"""
def main():
    config = configparser.ConfigParser()
    # Open and read config file to retrieve Redshift and DWH details required to connect
    try:
        config.read('dwh.cfg')
    except Exception as e:
        error(f"Error reading config file {e}")
        sys.exit()

    # Connect using cluster and DWH details
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    except Exception as e:
        error(f"Error connecting Data Warehouse: {e}")
        sys.exit()

    # Get conection cursor
    try:
        cur = conn.cursor()
    except Exception as e:
        error(f"Error getting connection cursor: {e}")
        conn.close()
        sys.exit()
    
    # Call Truncate function
    try:
        trunc_tables(cur, conn)
    except Exception as e:
        error(f"Error truncating table: {e}")
        conn.close()
        sys.exit()
        
    # Call COPY function
    try:
        copy_tables(cur, conn)
    except Exception as e:
        error(f"Error copying table: {e}")
        conn.close()
        sys.exit()

    # Quality Check - Zero Row count check
    table_list = ['total_population','race_population']
    for tbl in table_list:
        cur.execute(check_zero_count.format(tbl))
        result = cur.fetchone()
        if result[0] > 0:
            continue
        else:
            error(f"Zero row counts in {tbl}")
            conn.close()
            sys.exit()
        
    # Quality Check - Unique Key Check
    key_table_list = [['city || state_code', 'total_population']
                     ,['city || state_code || race', 'race_population']]
    for list in key_table_list:
        cur.execute(check_unique_key.format(list[0], list[0], list[1]))
        result = cur.fetchone()
        if result[0] == 0:
            continue
        else:
            error(f"Duplicate values in key, {list[0]}, in table {list[1]}")
            conn.close()
            sys.exit()

    # Close connection
    conn.close()

"""
  - Run above code if the file is labled __main__
  - Python internally labels files at runtime to differentiate between imported files and main file
"""
if __name__ == "__main__":
    main()