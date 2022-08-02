#!/usr/bin/env python3

from randomuser import RandomUser

# Gera uma lista com 5000 usuarios do Brasil
user_list = RandomUser.generate_users(10, {'nat': 'br'})

