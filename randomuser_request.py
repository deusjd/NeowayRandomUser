#!/usr/bin/env python3

import requests
import psycopg2
import json
from config import config

def create_tables():
    commands = (
        """
            create table pais (
            id serial primary key,
            nome varchar
            )
            """,
            """
            create table cidade (
            id serial primary key,
            pais_id integer,
            nome varchar,
            foreign key (pais_id) references pais (id)
            )
            """,
            """
            create table tipo_logradouro (
            id serial primary key,
            nome varchar
            )
            """,
            """
            create table logradouro (
            id serial primary key,
            cidade_id integer,
            tipo_logradouro_id integer,
            nome varchar,
            cep varchar,
            latitude decimal(11,8),
            longitude decimal(11,8),
            foreign key (cidade_id) references cidade (id),
            foreign key (tipo_logradouro_id) references tipo_logradouro (id)
            )
            """,
            """
            create table usuario (
            id serial primary key,
            logradouro_id integer,
            cpf varchar,
            nome varchar,
            sbrenome varchar,
            sexo varchar,
            data_nascimento timestamp,
            email varchar,
            telefone varchar,
            celular varchar,
            naturalidade varchar,
            logradouro_numero integer,
            data_cadastro timestamp,
            endereco_completo varchar,
            idade int,
            classificacao_etaria varchar generated always as (case when idade < 12 then 'CRIANÇA'
                                                                when idade between 12 and 18 then 'ADOLESCENTE'
                                                                when idade > 18 then 'ADULTO' END) stored,
            finalidade_uso varchar generated always as (case when idade > 18 then 'PRODUTOS DE MARKETING'
                                                                else 'PRODUTOS DE RISCO' END) stored,
            foreign key (logradouro_id) references logradouro (id)
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

def normalize_data():
    commands = ("""
            insert into pais (nome)  
            select distinct trim(location_country)  
              from users
              order by 1
              """
            , """
            insert into cidade (nome, pais_id)  
            select distinct trim(location_city) , p.id
              from users u
              join pais p on u.location_country = p.nome
              order by 1 ;
              """
            , """
             insert into tipo_logradouro (nome)
             select distinct trim(split_part(location_street_name,' ',1)) from users 
             order by 1
              """
            , """
             insert into logradouro (cidade_id, tipo_logradouro_id, nome, cep, latitude, longitude)
             select distinct c.id 
                           , tl.id
                           , trim(replace(u.location_street_name,trim(split_part(u.location_street_name,' ',1)),''))
                           , u.location_postcode
                           , u.location_coordinates_latitude 
                           , u.location_coordinates_longitude  
               from users u 
               left join cidade c on u.location_city = c.nome 
               left join tipo_logradouro tl on trim(split_part(u.location_street_name,' ',1)) = tl.nome 
               order by 3
              """
            , """
            insert into usuario (logradouro_id, cpf, nome, sbrenome, sexo, data_nascimento, email, telefone, celular, naturalidade, logradouro_numero, data_cadastro, endereco_completo, idade)
            select l.id
                , u.id_value
                , u.name_first 
                , u.name_last 
                , case u.gender when 'female' then 'FEMININO'
                            when 'male'	 then 'MASCULINO' end
            , u.dob_date 
            , email 
            , phone 
            , cell 
            , nat 
            , location_street_number 
            , now() 
            , concat(u.location_street_name,' ', u.location_street_number, ', ', u.location_postcode, ', ', u.location_city, ', ', u.location_country)
            , date_part('year',now())-date_part('year',u.dob_date)
            from users u 
            left join logradouro l on trim(replace(u.location_street_name,trim(split_part(u.location_street_name,' ',1)),'')) = l.nome 
                                and u.location_postcode = l.cep
                                and u.location_coordinates_latitude  = l.latitude 
                                and location_coordinates_longitude = l.longitude 
            left join tipo_logradouro tl on l.tipo_logradouro_id = tl.id
                left join cidade c on l.cidade_id = c.id 
                order by 3
            """)
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

if __name__ == '__main__':
    create_tables()
    insert_users()
    normalize_data()