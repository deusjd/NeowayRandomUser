#!/usr/bin/python

import psycopg2
from config import config

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