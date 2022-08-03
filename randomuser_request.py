#!/usr/bin/env python3

import requests
import psycopg2
import json
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
                registered_date timestamp,
                id_name varchar,
                id_value varchar
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
                                    registered_date,
                                    id_name,
                                    id_value)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING users_id"""

    users_id = None

    api_url = "https://randomuser.me/api/?noinfo&inc=gender,name,dob,email,phone,cell,nat,location,city,registered,id&results=5000&nat=br"

    response = requests.get(api_url)

    json_list = response.json()

    for x in json_list:
        value = json_list[x]
        for i in value:

            try:
                params = config()
                conn = psycopg2.connect(**params)
                cur = conn.cursor()
                cur.execute(sql_inset, [(i['gender'],),
                                        (i['name']['first'],),
                                        (i['name']['last'],),
                                        (i['dob']['date'],),
                                        (i['email'],),
                                        (i['phone'],),
                                        (i['cell'],),
                                        (i['nat'],),
                                        (i['location']['street']['name'],),
                                        (i['location']['street']['number'],),
                                        (i['location']['city'],),
                                        (i['location']['postcode'],),
                                        (i['location']['country'],),
                                        (i['location']['coordinates']['latitude'],),
                                        (i['location']['coordinates']['longitude'],),
                                        (i['registered']['date'],),
                                        (i['id']['name'],),
                                        (i['id']['value'],)])
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