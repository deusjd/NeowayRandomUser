#!/usr/bin/env python3

from randomuser import RandomUser
import psycopg2
from config import config

def create_database():
    commands = (
        """create database if not exists randomuser"""
    )

    conn = None

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS vendors (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE IF NOT EXISTS USERS (
                users_id SERIAL PRIMARY KEY,
                first_name VARCHAR(255) NOT NULL
                )
        """)

    conn = None

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_users():
   
    user_list = RandomUser.generate_users(5000, {'nat': 'br'})

    sql = """INSERT INTO users(first_name)
                VALUES (%s) RETURNING users_id"""

    users_id = None

    for x in user_list:

        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(sql, (x.get_first_name(),))
            vendor_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

if __name__ == '__main__':
    create_database()
    create_tables()
    insert_users()