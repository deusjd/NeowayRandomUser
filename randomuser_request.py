#!/usr/bin/env python3

import requests
import psycopg2
import json
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

def normalize_data():
    commands = ("""
            insert into pais (nome)  
            select distinct trim(location_country)  
              from users
              order by 1
              """
            , """
            insert into cidade (nome, estado,  pais_id)  
            select distinct trim(location_city), trim(location_state) , p.id
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
               left join cidade c on trim(u.location_city) = c.nome and trim(u.location_state) = c.estado 
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
            , concat(u.location_street_name,' ', u.location_street_number, ', ', u.location_postcode, ', ', u.location_city, ', ', u.location_state, ', ', u.location_country)
            , date_part('year',now())-date_part('year',u.dob_date)
            from users u 
            left join logradouro l on trim(replace(u.location_street_name,trim(split_part(u.location_street_name,' ',1)),'')) = l.nome 
                                and u.location_postcode = l.cep
                                and u.location_coordinates_latitude  = l.latitude 
                                and location_coordinates_longitude = l.longitude 
            left join tipo_logradouro tl on l.tipo_logradouro_id = tl.id
            left join cidade c on l.cidade_id = c.id 
            where u.location_state = c.estado 
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

def profiling():
    command = (
        """
                create table profiling as
        -- Regra 1 - 'Quantidade Tabelas'
            select 'Regra 1 - Quantidade Tabelas' regra
                , null tabela
                , null coluna
                , count(*)::text valor
                , (count(*)/(select count(*) from information_schema."tables" t where table_schema = 'public')*100)::numeric(5,2) preenchimento
            from information_schema."tables" t 
            where table_schema = 'public'	  
            union
        -- Regra 2 - 'Quantitativo Tabelas schema public'
        select 'Regra 2 - Quantitativo Tabelas schema public' regra
            , table_name tabela
            , null coluna
            , (xpath('/row/cnt/text()', xml_count))[1]::text as valor
            , '100'::numeric(5,2) preenchimento
        from (
        select table_name, table_schema, 
                query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
        from information_schema.tables
        where table_schema = 'public' --<< change here for the schema you want
        ) t
        union
        -- Regra 3 - 'Preenchimento'
        select 'Regra 3 - Preenchimento '||initcap(tabela) regra, tabela, trim(coluna), qtd valor, (qtd::int/(select count(*) from cidade)*100)  preenchimento from (with a as( Select unnest(array[concat('cidade - ','id - ',  count(id)) ,concat('cidade - ','nome - ',  count(nome)) ,concat('cidade - ','pais_id - ',  count(pais_id)) ,concat('cidade - ','estado - ',  count(estado))]) b from public.cidade) select  split_part(b,'-',1) tabela ,  split_part(b,'-',2) coluna ,  split_part(b,'-',3) qtd  from a) a union
        select 'Regra 3 - Preenchimento '||initcap(tabela) regra, tabela, trim(coluna), qtd valor, (qtd::int/(select count(*) from logradouro)*100)  preenchimento from (with a as( Select unnest(array[concat('logradouro - ','nome - ',  count(nome)) ,concat('logradouro - ','longitude - ',  count(longitude)) ,concat('logradouro - ','tipo_logradouro_id - ',  count(tipo_logradouro_id)) ,concat('logradouro - ','cep - ',  count(cep)) ,concat('logradouro - ','latitude - ',  count(latitude)) ,concat('logradouro - ','cidade_id - ',  count(cidade_id)) ,concat('logradouro - ','id - ',  count(id))]) b from public.logradouro) select  split_part(b,'-',1) tabela ,  split_part(b,'-',2) coluna ,  split_part(b,'-',3) qtd  from a) a union
        select 'Regra 3 - Preenchimento '||initcap(tabela) regra, tabela, trim(coluna), qtd valor, (qtd::int/(select count(*) from pais)*100)  preenchimento from (with a as( Select unnest(array[concat('pais - ','id - ',  count(id)) ,concat('pais - ','nome - ',  count(nome))]) b from public.pais) select  split_part(b,'-',1) tabela ,  split_part(b,'-',2) coluna ,  split_part(b,'-',3) qtd  from a) a union
        select 'Regra 3 - Preenchimento '||initcap(tabela) regra, tabela, trim(coluna), qtd valor, (qtd::int/(select count(*) from tipo_logradouro)*100)  preenchimento from (with a as( Select unnest(array[concat('tipo_logradouro - ','nome - ',  count(nome)) ,concat('tipo_logradouro - ','id - ',  count(id))]) b from public.tipo_logradouro) select  split_part(b,'-',1) tabela ,  split_part(b,'-',2) coluna ,  split_part(b,'-',3) qtd  from a) a union
        select 'Regra 3 - Preenchimento '||initcap(tabela) regra, tabela, trim(coluna), qtd valor, (qtd::int/(select count(*) from users)*100)  preenchimento from (with a as( Select unnest(array[concat('users - ','users_id - ',  count(users_id)) ,concat('users - ','id_name - ',  count(id_name)) ,concat('users - ','location_country - ',  count(location_country)) ,concat('users - ','location_state - ',  count(location_state)) ,concat('users - ','api_request - ',  count(api_request)) ,concat('users - ','dob_date - ',  count(dob_date)) ,concat('users - ','location_street_number - ',  count(location_street_number)) ,concat('users - ','location_coordinates_longitude - ',  count(location_coordinates_longitude)) ,concat('users - ','location_street_name - ',  count(location_street_name)) ,concat('users - ','location_city - ',  count(location_city)) ,concat('users - ','registered_date - ',  count(registered_date)) ,concat('users - ','gender - ',  count(gender)) ,concat('users - ','id_value - ',  count(id_value)) ,concat('users - ','name_first - ',  count(name_first)) ,concat('users - ','location_postcode - ',  count(location_postcode)) ,concat('users - ','nat - ',  count(nat)) ,concat('users - ','phone - ',  count(phone)) ,concat('users - ','location_coordinates_latitude - ',  count(location_coordinates_latitude)) ,concat('users - ','name_last - ',  count(name_last)) ,concat('users - ','email - ',  count(email)) ,concat('users - ','cell - ',  count(cell))]) b from public.users) select  split_part(b,'-',1) tabela ,  split_part(b,'-',2) coluna ,  split_part(b,'-',3) qtd  from a) a union
        select 'Regra 3 - Preenchimento '||initcap(tabela) regra, tabela, trim(coluna), qtd valor, (qtd::int/(select count(*) from usuario)*100)  preenchimento from (with a as( Select unnest(array[concat('usuario - ','email - ',  count(email)) ,concat('usuario - ','endereco_completo - ',  count(endereco_completo)) ,concat('usuario - ','celular - ',  count(celular)) ,concat('usuario - ','logradouro_id - ',  count(logradouro_id)) ,concat('usuario - ','cpf - ',  count(cpf)) ,concat('usuario - ','data_nascimento - ',  count(data_nascimento)) ,concat('usuario - ','telefone - ',  count(telefone)) ,concat('usuario - ','nome - ',  count(nome)) ,concat('usuario - ','naturalidade - ',  count(naturalidade)) ,concat('usuario - ','finalidade_uso - ',  count(finalidade_uso)) ,concat('usuario - ','classificacao_etaria - ',  count(classificacao_etaria)) ,concat('usuario - ','sbrenome - ',  count(sbrenome)) ,concat('usuario - ','data_cadastro - ',  count(data_cadastro)) ,concat('usuario - ','logradouro_numero - ',  count(logradouro_numero)) ,concat('usuario - ','sexo - ',  count(sexo)) ,concat('usuario - ','idade - ',  count(idade)) ,concat('usuario - ','id - ',  count(id))]) b from public.usuario) select  split_part(b,'-',1) tabela ,  split_part(b,'-',2) coluna ,  split_part(b,'-',3) qtd  from a) a
        union
        -- Regra 4 - 'CPFs Inválidos'
        select 'Regra 4 - CPFs Inválidos Usuario' regra
            , 'Usuario' tabela
            , 'cpf' coluna
            , (referencia::text||' - '||qtd::text) valor
            , (qtd/(select count(*) from usuario u)::numeric*100) preenchimento
        from 
        (select case fn_cnpj_cpf(regexp_replace(cpf, '\D','','g')) when true then 'VALIDO'
                                                                when false then 'INVALIDO' end referencia
            , count(*) qtd  
        from usuario u  
        group by 1) a
        order by 1
        """
        )

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
            
if __name__ == '__main__':
    create_tables()
    insert_users()
    normalize_data()
    profiling()