#!/usr/bin/env python3

from distutils.cmd import Command
from imaplib import Commands
from time import timezone
from randomuser import RandomUser
import psycopg2
from config import config


def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS vendors (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE IF NOT EXISTS USERS (
                users_id serial primary key, 
                gender varchar,
                name_first varchar,
                name_last varchar,
                dob_date timestamp,
                email varchar,
                phone varchar,
                cell varchar,
                nat varchar,
                location_street_name varchar,
                location_street_number integer,
                location_city varchar,
                location_postcode varchar,
                location_country varchar,
                location_coordinates_latitude decimal(11,8),
                location_coordinates_longitude decimal(11,8),
                registered_date timestamp
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

    sql_inset = """INSERT INTO users(gender,
                                    name_first,
                                    name_last,
                                    dob_date,
                                    email,
                                    phone,
                                    cell,
                                    nat,
                                    location_street_name,
                                    location_street_number,
                                    location_city,
                                    location_postcode,
                                    location_country,
                                    location_coordinates_latitude,
                                    location_coordinates_longitude,
                                    registered_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING users_id"""

    users_id = None


    for x in user_list:
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(sql_inset, [(x.get_gender(),),(x.get_first_name(),),(x.get_last_name(),),(x.get_dob(),),(x.get_email(),),(x.get_phone(),),(x.get_cell(),),(x.get_nat(),),(x.get_street(split_number_name=True)['name'],),(x.get_street(split_number_name=True)['number'],),(x.get_city(),),(x.get_zipcode(),),(x.get_country(),),(x.get_coordinates()['latitude'],),(x.get_coordinates()['longitude'],),(x.get_registered(),)])
            users_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
    


if __name__ == '__main__':
    create_tables()
    insert_users()