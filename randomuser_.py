#!/usr/bin/env python3

from create_database import create_database
from create_tables import create_tables
from insert_user import insert_users
from normalize_data import normalize_data
from profiling import profiling
from create_fn_cpf import create_fn_cpf

            
if __name__ == '__main__':
    create_database()
    create_fn_cpf()
    create_tables()
    insert_users()
    normalize_data()
    profiling()