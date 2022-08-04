#!/usr/bin/python

import requests
import psycopg2
from config import config

def insert_users():

    sql_insert = """INSERT INTO users(gender,
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
                                    location_state,
                                    location_postcode,
                                    location_country,
                                    location_coordinates_latitude,
                                    location_coordinates_longitude,
                                    registered_date,
                                    id_name,
                                    id_value,
                                    api_request)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING users_id"""

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
                cur.execute(sql_insert, [(i['gender'],),
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
                                        (i['location']['state'],),
                                        (i['location']['postcode'],),
                                        (i['location']['country'],),
                                        (i['location']['coordinates']['latitude'],),
                                        (i['location']['coordinates']['longitude'],),
                                        (i['registered']['date'],),
                                        (i['id']['name'],),
                                        (i['id']['value'],),
                                        (api_url,)])
                users_id = cur.fetchone()[0]
                conn.commit()
                cur.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                if conn is not None:
                    conn.close()
