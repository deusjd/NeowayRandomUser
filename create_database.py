#!/usr/bin/python

import psycopg2
from config import config_postgres
from psycopg2 import connect, extensions, sql

def create_database():
    command = (
        """	SELECT pid, pg_terminate_backend(pid) 
            FROM pg_stat_activity WHERE datname = 'random_user';

            DROP DATABASE IF EXISTS dbname;
            CREATE DATABASE dbname;"""
    )

    autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT

    try:
        params = config_postgres()
        conn = psycopg2.connect(**params)
        conn.set_isolation_level( autocommit )
        cur = conn.cursor()
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier( 'random_user' )))
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()