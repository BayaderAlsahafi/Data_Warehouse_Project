"""Create Tables for the Sparkify Database

This script creates the required tables in the Sparkify database.

This script requires you to have the sql_queries.py file in your project folder.

This script requires the following libraries to be installed within the Python
environment you are running this script in:
    * configparser
    * psycopg2
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ 
    A function to drop all the existing tables in the database.
    
    Parameters: 
    cur (cursor): Psycopg2 cursor to execute PostgreSQL command in a database session.
    conn (connection): Handles the connection to a PostgreSQL database instance.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
    A function to create all the tables in the database.
    
    Parameters: 
    cur (cursor): Psycopg2 cursor to execute PostgreSQL command in a database session.
    conn (connection): Handles the connection to a PostgreSQL database instance.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()