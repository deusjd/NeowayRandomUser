#!/usr/bin/python

import psycopg2
from config import config

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