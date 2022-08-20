import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

'''this file will do all the magic from staging, to inserting all our data to
destination DB.'''

def load_staging_tables(cur, conn):
    '''this method will take all the copy queries, just as defined in our sql_queries.py
    and will loop through them and execute all of them'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''this method will insert all the data provided fron staging table to our RDS table.
    will loop through all sql queries and execute all them, to insert the data.'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''this is the main method, which we willimport the dwh.cfg info hear so we can use the DB info and connect to the db, also we will establish a connection, call the first method  "load_staging_tables" to load the data to the staging table from the S3 bucket, then call the second method "insert_tables" to insert them in our new DB.'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()