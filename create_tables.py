import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
'''
this file will take the "create_table_queries" and "drop_table_queries" lists from sql queries
will apply "drop_tables" method and "create_table" methods
'''

def drop_tables(cur, conn):
    '''this method wil take cursor and connection as inputs which are basiclly
    DB info, and then loop through all drop queries in sql_queries.py'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''this method wil take cursor and connection as inputs which are basiclly
    DB info, and loop through all create tables queries we defined in sql_queries.py'''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''this section is main, we first will get config file "dwh.cfg.
    and then connect to my db, and pass all the info needed fetched from dwh.cfg
    then we will call "drop_tables" method to drop all tables in order to restart our work.
    after that we will call "create_tables" method that will create our tables just as defined in our sql queries.py. and finally, close the connection.'''
    print(" flag 111")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print(" flag 111")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(" flag 111")
    drop_tables(cur, conn)
    # create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()