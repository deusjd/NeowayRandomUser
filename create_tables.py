#!/usr/bin/python

import psycopg2
from config import config

def create_tables():
    commands = (
        """
            CREATE TABLE IF NOT EXISTS pais (
            id serial primary key,
            nome varchar
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS cidade (
            id serial primary key,
            pais_id integer,
            nome varchar,
            estado varchar,
            foreign key (pais_id) references pais (id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tipo_logradouro (
            id serial primary key,
            nome varchar
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS logradouro (
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
            CREATE TABLE IF NOT EXISTS usuario (
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
                location_state varchar,
                location_postcode varchar,
                location_country varchar,
                location_coordinates_latitude decimal(11,8),
                location_coordinates_longitude decimal(11,8),
                registered_date timestamp,
                id_name varchar,
                id_value varchar,
                api_request varchar
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