"""ETL for the Sparkify Database

This script performs the ETL pipeline for a database hosted on Redshift.

This script requires you to run the create_tables.py script first, and to have the sql_queries.py file in your project folder.

This script requires the following libraries to be installed within the Python
environment you are running this script in:
    * configparser
    * psycopg2
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ 
    A function that load song and event data files into the corresponding staging tables in the database.
    
    Parameters: 
    cur (cursor): Psycopg2 cursor to execute PostgreSQL command in a database session.
    conn (connection): Handles the connection to a PostgreSQL database instance.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ 
    A function to transform and load data into the tables in the database.
    
    Parameters: 
    cur (cursor): Psycopg2 cursor to execute PostgreSQL command in a database session.
    conn (connection): Handles the connection to a PostgreSQL database instance.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()