import os
# import git
import json
import pandas as pd
import mysql.connector
import sqlalchemy
# from sqlalchemy.exc import IntegrityError
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import numpy as np

# # Clone the PhonePe Pulse repository
# repo_url = 'https://github.com/phonepe/pulse.git'
# repo_dir = 'pulse'
# git.Repo.clone_from(repo_url, repo_dir)

# Access the repository directory
# repo_path = os.path.join(os.getcwd(), repo_dir)
repo_path = 'D:/CNR/GUVI DS/Projects/2. PhonePe/coding/pycharm/pulse'
os.chdir(repo_path)

if 'dts_' not in st.session_state:
    # Reading the state geojson
    with open("D:/CNR/GUVI DS/Projects/2. PhonePe/coding/pycharm/ind_state_boundaries.geojson", 'r') as g_json:
        new_indian_state = json.load(g_json)
    st.session_state['n_i_s'] = new_indian_state

    # Adding the required features
    state_s = []
    for s_t in new_indian_state['features']:
        sta_te = {"id": s_t['id'], "name": s_t['properties']['shapeName']}
        state_s.append(sta_te)
    # creating dataframe
    df = pd.DataFrame(state_s)
    # applying necessary changes
    df.at[35, 'name'] = 'Haryana'
    df.at[25, 'name'] = 'Uttarakhand'
    df.at[4, 'name'] = 'Uttar-Pradesh'
    df.at[26, 'name'] = 'Dadra and Nagar Haveli and Damān and Diu'
    # sorting the dataframe with respect to name
    sorted_df = df.sort_values('name')
    sorted_df_with_ordered_index = sorted_df.reset_index(drop=True)
    # id dataframe
    id_d = pd.DataFrame(sorted_df_with_ordered_index['id'])
    st.session_state['id_d'] = id_d

    # Reading the district geojson
    with open('D:/CNR/GUVI DS/Projects/2. PhonePe/coding/pycharm/ind_dt_boundaries.geojson', 'r') as dis:
        ind_d = json.load(dis)
    st.session_state['ind_d'] = ind_d

    dts_ = {}
    for dt in ind_d['features']:
        dts_[dt['properties']['shapeName'].lower()] = dt['id']
    st.session_state['dts_'] = dts_

# SQL Connector
ppdb = mysql.connector.connect(host='localhost',
                               user='root',
                               password='password',
                               database='phonepe_pulse')
cursor = ppdb.cursor()


# cursor.execute("create database ppdb")
# ppdb.commit()

# cursor.execute("""create table aggregated_transaction (year INT,
#                                                        quarterly INT,
#                                                        payment varchar(255),
#                                                        type varchar(255),
#                                                        payment_count INT,
#                                                        total_amount DECIMAL(15, 2) )""")
# ppdb.commit()

# cursor.execute("""create table aggregated_transaction_states (state varchar(255),
#                                                               year INT,
#                                                               quarterly INT,
#                                                               payment varchar(255),
#                                                               type varchar(255),
#                                                               payment_count INT,
#                                                               total_amount DECIMAL(15, 2))""")
# ppdb.commit()

# cursor.execute("""create table aggregated_users (year INT,
#                                                  quarterly INT,
#                                                  registeredUsers INT,
#                                                  appOpens INT,
#                                                  brand varchar(255),
#                                                  count INT,
#                                                  percentage FLOAT)""")
# ppdb.commit()

# cursor.execute("""create table aggregated_users_state (state varchar(255),
#                                                        year INT,
#                                                          quarterly INT,
#                                                          registeredUsers INT,
#                                                          appOpens INT,
#                                                          brand varchar(255),
#                                                          count INT,
#                                                          percentage FLOAT)""")
# ppdb.commit()

# cursor.execute("""create table map_transactions (year INT,
#                                                  quarterly INT,
#                                                  state varchar(255),
#                                                  type varchar(255),
#                                                  count INT,
#                                                  amount DECIMAL(15, 2))""")
# ppdb.commit()

# cursor.execute("""create table map_transactions_st (state varchar(255),
#                                                     year INT,
#                                                     quarterly INT,
#                                                     district varchar(255),
#                                                     type varchar(255),
#                                                     count INT,
#                                                     amount DECIMAL(15, 2))""")
# ppdb.commit()

# cursor.execute("""create table map_users (year INT,
#                                           quarterly INT,
#                                           state varchar(255),
#                                           registeredUsers INT,
#                                           appOpens INT)""")
# ppdb.commit()

# cursor.execute("""create table map_users_states (state varchar(255),
#                                                 year INT,
#                                                   quarterly INT,
#                                                   district varchar(255),
#                                                   registeredUsers INT,
#                                                   appOpens INT)""")
# ppdb.commit()

# cursor.execute("""create table top_trs_in_states (year INT,
#                                                   quarterly INT,
#                                                   state varchar(255),
#                                                   type varchar(255),
#                                                   count INT,
#                                                   amount DECIMAL(15, 2))""")
# ppdb.commit()

# cursor.execute("""create table top_trs_in_districts (year INT,
#                                                      quarterly INT,
#                                                      district varchar(255),
#                                                      type varchar(255),
#                                                      count INT,
#                                                      amount DECIMAL(15, 2))""")

# ppdb.commit()

# cursor.execute("""create table top_trs_in_pincodes (year INT,
#                                                      quarterly INT,
#                                                      pincode INT,
#                                                      type varchar(255),
#                                                      count INT,
#                                                      amount DECIMAL(15, 2))""")

# ppdb.commit()

# cursor.execute("""create table top_trs_st_districts (state varchar(255),
#                                                      year INT,
#                                                      quarterly INT,
#                                                      district varchar(255),
#                                                      type varchar(255),
#                                                      count INT,
#                                                      amount DECIMAL(15, 2))""")
# ppdb.commit()

# cursor.execute("""create table top_trs_st_pincodes (state varchar(255),
#                                                      year INT,
#                                                      quarterly INT,
#                                                      pincode INT,
#                                                      type varchar(255),
#                                                      count INT,
#                                                      amount DECIMAL(15, 2))""")
# ppdb.commit()

# cursor.execute("""create table top_in_st_users (year INT,
#                                                 quarterly INT,
#                                                 state varchar(255),
#                                                 registeredUsers INT)""")
# ppdb.commit()

# cursor.execute("""create table top_in_dt_users (year INT,
#                                                 quarterly INT,
#                                                 district varchar(255),
#                                                 registeredUsers INT)""")
# ppdb.commit()

# cursor.execute("""create table top_in_pin_users (year INT,
#                                                 quarterly INT,
#                                                 pincode INT,
#                                                 registeredUsers INT)""")
# ppdb.commit()

# cursor.execute("""create table top_st_dt_users (state varchar(255),
#                                                 year INT,
#                                                 quarterly INT,
#                                                 district varchar(255),
#                                                 registeredUsers INT)""")
# ppdb.commit()

# cursor.execute("""create table top_st_pin_users (state varchar(255),
#                                                 year INT,
#                                                 quarterly INT,
#                                                 pincode INT,
#                                                 registeredUsers INT)""")
# ppdb.commit()

def df_sql():
    # 1.1 Aggregate Transactions India
    aggregated_transaction = []
    country_path = 'data/aggregated/transaction/country/india'
    year_path = ''
    for year in os.listdir(country_path):
        if year.isdigit():
            year_path = f'data/aggregated/transaction/country/india/{year}'
        qtrly = 0
        for file in os.listdir(year_path):
            qtrly += 1
            # Specify the file path to extract from the repository
            file_path = f'{year_path}/{file}'
            # Load the JSON file data
            with open(file_path, 'r') as doc:
                json_data = json.load(doc)
            if year.isdigit():
                for data in json_data['data']['transactionData']:
                    tr_data = {'year': int(year), 'quarterly': qtrly, 'payment': data['name'],
                               'type': data['paymentInstruments'][0]['type'],
                               'payment_count': data['paymentInstruments'][0]['count'],
                               'total_amount': float(data['paymentInstruments'][0]['amount'])}
                    aggregated_transaction.append(tr_data)
    aggregated_transaction_df = pd.DataFrame(aggregated_transaction)

    # 1.2 Aggregate Transactions States
    state_path = 'data/aggregated/transaction/country/india/state'
    aggregated_transaction_states = []
    for state in os.listdir(state_path):
        state_path = f'data/aggregated/transaction/country/india/state/{state}'
        for year in os.listdir(state_path):
            if year.isdigit():
                year_path = f'{state_path}/{year}'
            qtrly = 0
            for file in os.listdir(year_path):
                qtrly += 1
                # Specify the file path to extract from the repository
                file_path = f'{year_path}/{file}'

                # Load the JSON file data
                with open(file_path, 'r') as doc:
                    json_data = json.load(doc)
                for data in json_data['data']['transactionData']:
                    tr_data = {'state': state, 'year': int(year), 'quarterly': qtrly, 'payment': data['name'],
                               'type': data['paymentInstruments'][0]['type'],
                               'payment_count': data['paymentInstruments'][0]['count'],
                               'total_amount': float(data['paymentInstruments'][0]['amount'])}
                    aggregated_transaction_states.append(tr_data)
    aggregated_transaction_states_df = pd.DataFrame(aggregated_transaction_states)

    # 1.3 Aggregated User Details_India

    # Specifying the path of aggregated users' detail
    user_path = f'data/aggregated/user/country/india'
    aggregated_users = []
    for year in os.listdir(user_path):
        if year.isdigit():
            # Specifying the path of specific year for the aggregated users' detail
            year_path = f'data/aggregated/user/country/india/{year}'
        qtrly = 0
        for file in os.listdir(year_path):
            qtrly += 1
            # Specifying the the particular file path
            file_path = f'{year_path}/{file}'
            with open(file_path, 'r') as doc:
                user_json = json.load(doc)
            registeredusers = user_json['data']['aggregated']['registeredUsers']
            appopens = user_json['data']['aggregated']['appOpens']
            if user_json['data']['usersByDevice'] and year.isdigit():
                for data in user_json['data']['usersByDevice']:
                    aggregated_user = {'year': int(year), 'quarterly': qtrly, 'registeredUsers': registeredusers,
                                       'appOpens': appopens, 'brand': data['brand'], 'count': data['count'],
                                       'percentage': data['percentage'] * 100}
                    aggregated_users.append(aggregated_user)
            else:
                if year.isdigit():
                    aggregated_user = {'year': int(year), 'quarterly': qtrly, 'registeredUsers': registeredusers,
                                       'appOpens': appopens, 'brand': 'Others', 'count': 0, 'percentage': 0}
                    aggregated_users.append(aggregated_user)
    aggregated_users_df = pd.DataFrame(aggregated_users)

    # 1.4 Aggregated User Details_States

    user_state_path = 'data/aggregated/user/country/india/state'
    u_s_year = ''
    aggregated_user_states = []
    for state in os.listdir(user_state_path):
        user_state_path = f'data/aggregated/user/country/india/state/{state}'
        for year in os.listdir(user_state_path):
            if year.isdigit():
                u_s_year = f'{user_state_path}/{year}'
            qtrly = 0
            for file in os.listdir(u_s_year):
                qtrly += 1
                u_s_file_path = f'{u_s_year}/{file}'
                with open(u_s_file_path, 'r') as doc:
                    user_json = json.load(doc)
                registeredusers = user_json['data']['aggregated']['registeredUsers']
                appopens = user_json['data']['aggregated']['appOpens']
                if user_json['data']['usersByDevice']:
                    for data in user_json['data']['usersByDevice']:
                        aggregated_user = {'state': state, 'year': int(year), 'quarterly': qtrly,
                                           'registeredUsers': registeredusers, 'appOpens': appopens,
                                           'brand': data['brand'],
                                           'count': data['count'], 'percentage': data['percentage'] * 100}
                        aggregated_user_states.append(aggregated_user)
                else:
                    aggregated_user = {'state': state, 'year': int(year), 'quarterly': qtrly,
                                       'registeredUsers': registeredusers, 'appOpens': appopens, 'brand': 'Others',
                                       'count': 0, 'percentage': 0}
                    aggregated_user_states.append(aggregated_user)
    aggregated_users_state_df = pd.DataFrame(aggregated_user_states)

    # 2.1 Map_Transaction_Details_India

    map_transactions = []
    map_tr_in_path = 'data/map/transaction/hover/country/india'
    map_tr_in_year_path = ''
    for year in os.listdir(map_tr_in_path):
        if year.isdigit():
            map_tr_in_year_path = f'{map_tr_in_path}/{year}'
        qtrly = 0
        for file in os.listdir(map_tr_in_year_path):
            qtrly += 1
            map_tr_in_file_path = f'{map_tr_in_year_path}/{file}'

            with open(map_tr_in_file_path, 'r') as doc:
                map_tr = json.load(doc)

            if year.isdigit():
                for state in map_tr['data']['hoverDataList']:
                    map_transaction = {'year': int(year), 'quarterly': qtrly, 'state': state['name'],
                                       'type': state['metric'][0]['type'], 'count': state['metric'][0]['count'],
                                       'amount': float(state['metric'][0]['amount'])}
                    map_transactions.append(map_transaction)
    map_transactions_df = pd.DataFrame(map_transactions)

    # 2.2 Map_Transaction_Details_States
    map_transactions_st = []
    map_tr_st_path = 'data/map/transaction/hover/country/india/state'
    map_tr_st_year_path = ''
    for state in os.listdir(map_tr_st_path):
        map_tr_st_path = f'data/map/transaction/hover/country/india/state/{state}'
        for year in os.listdir(map_tr_st_path):
            if year.isdigit():
                map_tr_st_year_path = f'{map_tr_st_path}/{year}'
            qtrly = 0
            for file in os.listdir(map_tr_st_year_path):
                qtrly += 1
                map_tr_st_file_path = f'{map_tr_st_year_path}/{file}'

                with open(map_tr_st_file_path, 'r') as doc:
                    map_tr_st = json.load(doc)
                #                 print(map_tr_st)
                #                 print('\n')

                if year.isdigit():
                    for district in map_tr_st['data']['hoverDataList']:
                        map_transaction = {'state': state, 'year': int(year), 'quarterly': qtrly,
                                           'district': district['name'], 'type': district['metric'][0]['type'],
                                           'count': district['metric'][0]['count'],
                                           'amount': float(district['metric'][0]['amount'])}
                        map_transactions_st.append(map_transaction)
    map_transactions_st_df = pd.DataFrame(map_transactions_st)

    # 2.3 Map_Users'_Details_India
    map_users = []
    map_user_in_path = 'data/map/user/hover/country/india'
    map_user_in_year_path = ''
    for year in os.listdir(map_user_in_path):
        if year.isdigit():
            map_user_in_year_path = f'{map_user_in_path}/{year}'
        qtrly = 0
        for file in os.listdir(map_user_in_year_path):
            qtrly += 1
            map_user_in_file_path = f'{map_user_in_year_path}/{file}'

            with open(map_user_in_file_path, 'r') as doc:
                map_user_json = json.load(doc)
            #             print(map_user)
            #             print('\n')
            states = list(map_user_json['data']['hoverData'].keys())
            if year.isdigit():
                for state in states:
                    map_user = {'year': int(year), 'quarterly': qtrly, 'state': state,
                                'registeredUsers': map_user_json['data']['hoverData'][state]['registeredUsers'],
                                'appOpens': map_user_json['data']['hoverData'][state]['appOpens']}
                    map_users.append(map_user)
    map_users_df = pd.DataFrame(map_users)

    # 2.4 Map_Users'_Details_States
    map_users_states = []
    map_user_st_path = 'data/map/user/hover/country/india/state'
    for state in os.listdir(map_user_st_path):
        map_user_st_path = f'data/map/user/hover/country/india/state/{state}'
        for year in os.listdir(map_user_st_path):
            map_user_st_year_path = f'{map_user_st_path}/{year}'
            qtrly = 0
            for file in os.listdir(map_user_st_year_path):
                qtrly += 1
                map_user_st_file_path = f'{map_user_st_year_path}/{file}'

                with open(map_user_st_file_path, 'r') as doc:
                    map_user_state_json = json.load(doc)

                districts = list(map_user_state_json['data']['hoverData'].keys())
                for district in districts:
                    map_user_state = {'state': state, 'year': int(year), 'quarterly': qtrly, 'district': district,
                                      'registeredUsers': map_user_state_json['data']['hoverData'][district][
                                          'registeredUsers'],
                                      'appOpens': map_user_state_json['data']['hoverData'][district]['appOpens']}
                    map_users_states.append(map_user_state)
    map_users_states_df = pd.DataFrame(map_users_states)

    # 3.1 Top_Transaction_India
    top_trs_in_states = []
    top_trs_in_districts = []
    top_trs_in_pincodes = []
    top_tr_in = 'data/top/transaction/country/india'
    top_tr_in_year = ''
    for year in os.listdir(top_tr_in):
        if year.isdigit():
            top_tr_in_year = f'{top_tr_in}/{year}'
        qtrly = 0
        for file in os.listdir(top_tr_in_year):
            qtrly += 1
            top_tr_in_file = f'{top_tr_in_year}/{file}'

            with open(top_tr_in_file, 'r') as doc:
                top_tr_json = json.load(doc)

            if year.isdigit():
                # Top States in Transactions
                for state in top_tr_json['data']['states']:
                    top_in_state = {'year': int(year), 'quarterly': qtrly, 'state': state['entityName'],
                                    'type': state['metric']['type'], 'count': state['metric']['count'],
                                    'amount': float(state['metric']['amount'])}
                    top_trs_in_states.append(top_in_state)

                # Top Districts in Transactions
                for district in top_tr_json['data']['districts']:
                    top_in_district = {'year': int(year), 'quarterly': qtrly, 'district': district['entityName'],
                                       'type': district['metric']['type'], 'count': district['metric']['count'],
                                       'amount': float(district['metric']['amount'])}
                    top_trs_in_districts.append(top_in_district)

                # Top Pincodes in Transactions
                for pincode in top_tr_json['data']['pincodes']:
                    top_in_pincode = {'year': int(year), 'quarterly': qtrly, 'pincode': pincode['entityName'],
                                      'type': pincode['metric']['type'], 'count': pincode['metric']['count'],
                                      'amount': float(pincode['metric']['amount'])}
                    top_trs_in_pincodes.append(top_in_pincode)

    top_trs_in_states_df = pd.DataFrame(top_trs_in_states)
    top_trs_in_districts_df = pd.DataFrame(top_trs_in_districts)
    top_trs_in_pincodes_df = pd.DataFrame(top_trs_in_pincodes)

    # 3.2 Top Transactions_States
    top_trs_st_districts = []
    top_trs_st_pincodes = []
    top_tr_st = 'data/top/transaction/country/india/state'
    for state in os.listdir(top_tr_st):
        top_tr_sts = f'{top_tr_st}/{state}'
        for year in os.listdir(top_tr_sts):
            top_tr_st_year = f'{top_tr_sts}/{year}'
            qtrly = 0
            for file in os.listdir(top_tr_st_year):
                qtrly += 1
                top_tr_st_file = f'{top_tr_st_year}/{file}'

                with open(top_tr_st_file, 'r') as doc:
                    top_tr_st_json = json.load(doc)
                #                 print(top_tr_st_json)
                #                 print('\n')

                # Top Districts in Transactions
                for district in top_tr_st_json['data']['districts']:
                    top_st_district = {'state': state, 'year': int(year), 'quarterly': qtrly,
                                       'district': district['entityName'], 'type': district['metric']['type'],
                                       'count': district['metric']['count'],
                                       'amount': float(district['metric']['amount'])}
                    top_trs_st_districts.append(top_st_district)

                # Top Pincodes in Transactions
                for pincode in top_tr_st_json['data']['pincodes']:
                    top_st_pincode = {'state': state, 'year': int(year), 'quarterly': qtrly,
                                      'pincode': pincode['entityName'], 'type': pincode['metric']['type'],
                                      'count': pincode['metric']['count'], 'amount': float(pincode['metric']['amount'])}
                    top_trs_st_pincodes.append(top_st_pincode)

    top_trs_st_districts_df = pd.DataFrame(top_trs_st_districts)
    top_trs_st_pincodes_df = pd.DataFrame(top_trs_st_pincodes)

    # 3.3 Top_Users_India
    top_in_st_users = []
    top_in_dt_users = []
    top_in_pin_users = []
    top_users_in = 'data/top/user/country/india'
    top_users_in_year = ''
    for year in os.listdir(top_users_in):
        if year.isdigit():
            top_users_in_year = f'{top_users_in}/{year}'
        qtrly = 0
        for file in os.listdir(top_users_in_year):
            qtrly += 1
            top_users_in_file = f'{top_users_in_year}/{file}'

            with open(top_users_in_file, 'r') as doc:
                top_users_in_json = json.load(doc)

            if year.isdigit():
                # Top States - User count
                for state in top_users_in_json['data']['states']:
                    top_in_st_user = {'year': int(year), 'quarterly': qtrly, 'state': state['name'],
                                      'registeredUsers': state['registeredUsers']}
                    top_in_st_users.append(top_in_st_user)

                # Top Districts - User count
                for district in top_users_in_json['data']['districts']:
                    top_in_dt_user = {'year': int(year), 'quarterly': qtrly, 'district': district['name'],
                                      'registeredUsers': district['registeredUsers']}
                    top_in_dt_users.append(top_in_dt_user)

                # Top Districts - User count
                for pincode in top_users_in_json['data']['pincodes']:
                    top_in_pin_user = {'year': int(year), 'quarterly': qtrly, 'pincode': pincode['name'],
                                       'registeredUsers': pincode['registeredUsers']}
                    top_in_pin_users.append(top_in_pin_user)

    top_in_st_users_df = pd.DataFrame(top_in_st_users)
    top_in_dt_users_df = pd.DataFrame(top_in_dt_users)
    top_in_pin_users_df = pd.DataFrame(top_in_pin_users)

    # 3.4 Top_Users_States
    top_st_dt_users = []
    top_st_pin_users = []
    top_users_st = 'data/top/user/country/india/state'
    for state in os.listdir(top_users_st):
        top_users_sts = f'{top_users_st}/{state}'
        for year in os.listdir(top_users_sts):
            top_users_st_year = f'{top_users_sts}/{year}'
            qtrly = 0
            for file in os.listdir(top_users_st_year):
                qtrly += 1
                top_users_st_file = f'{top_users_st_year}/{file}'

                with open(top_users_st_file, 'r') as doc:
                    top_users_st_json = json.load(doc)

                # Top Districts - User count
                for district in top_users_st_json['data']['districts']:
                    top_st_dt_user = {'state': state, 'year': int(year), 'quarterly': qtrly,
                                      'district': district['name'],
                                      'registeredUsers': district['registeredUsers']}
                    top_st_dt_users.append(top_st_dt_user)

                # Top Districts - User count
                for pincode in top_users_st_json['data']['pincodes']:
                    top_st_pin_user = {'state': state, 'year': int(year), 'quarterly': qtrly,
                                       'pincode': pincode['name'],
                                       'registeredUsers': pincode['registeredUsers']}
                    top_st_pin_users.append(top_st_pin_user)

    top_st_dt_users_df = pd.DataFrame(top_st_dt_users)
    top_st_pin_users_df = pd.DataFrame(top_st_pin_users)

    # SQL alchemy connection
    engine = sqlalchemy.create_engine('mysql+pymysql://root:'password'@localhost:3306/phonepe_pulse')

    aggregated_transaction_df.to_sql('aggregated_transaction', engine, if_exists='append', index=False)
    aggregated_transaction_states_df.to_sql('aggregated_transaction_states', engine, if_exists='append',
                                            index=False)
    aggregated_users_df.to_sql('aggregated_users', engine, if_exists='append', index=False)
    aggregated_users_state_df.to_sql('aggregated_users_state', engine, if_exists='append', index=False)
    map_transactions_df.to_sql('map_transactions', engine, if_exists='append', index=False)
    map_transactions_st_df.to_sql('map_transactions_st', engine, if_exists='append', index=False)
    map_users_df.to_sql('map_users', engine, if_exists='append', index=False)
    map_users_states_df.to_sql('map_users_states', engine, if_exists='append', index=False)
    top_trs_in_states_df.to_sql('top_trs_in_states', engine, if_exists='append', index=False)
    top_trs_in_districts_df.to_sql('top_trs_in_districts', engine, if_exists='append', index=False)
    top_trs_in_pincodes_df.to_sql('top_trs_in_pincodes', engine, if_exists='append', index=False)
    top_trs_st_districts_df.to_sql('top_trs_st_districts', engine, if_exists='append', index=False)
    top_trs_st_pincodes_df.to_sql('top_trs_st_pincodes', engine, if_exists='append', index=False)
    top_in_st_users_df.to_sql('top_in_st_users', engine, if_exists='append', index=False)
    top_in_dt_users_df.to_sql('top_in_dt_users', engine, if_exists='append', index=False)
    top_in_pin_users_df.to_sql('top_in_pin_users', engine, if_exists='append', index=False)
    top_st_dt_users_df.to_sql('top_st_dt_users', engine, if_exists='append', index=False)
    top_st_pin_users_df.to_sql('top_st_pin_users', engine, if_exists='append', index=False)


def expensive_computation(selected_year, selected_quarter):
    # Perform expensive computation here
    new_indian_states = st.session_state['n_i_s']
    id_df = st.session_state['id_d']
    ind_dt = st.session_state['ind_d']
    dts = st.session_state['dts_']
    # Transaction map state wise
    query_1 = f'''select state as State, 
                count as All_Transactions, 
                amount as Total_Transaction_Value, 
                (amount DIV count) as Average_Transaction_Value 
                from map_transactions 
                where year = {selected_year} and quarterly =  {selected_quarter}
                ORDER BY state ASC'''
    query_df_1 = pd.read_sql_query(query_1, ppdb)
    # combing query_df with Id_df
    df_combined_1 = query_df_1.join(id_df)

    # Create the choropleth mapbox
    fig_1 = px.choropleth_mapbox(data_frame=df_combined_1,
                                 locations='id',
                                 geojson=new_indian_states,
                                 color='All_Transactions',
                                 color_continuous_scale='PiYG',
                                 hover_name='State',
                                 hover_data=['Total_Transaction_Value', 'Average_Transaction_Value'],
                                 mapbox_style='carto-positron',
                                 center={'lat': 24, 'lon': 78},
                                 zoom=3)

    # Transaction map district wise
    query_2 = f'''select district as District, 
                                    count as All_Transcations, 
                                    amount as Total_Transaction_Value, 
                                    (amount DIV count) as Average_Transaction_Value 
                                    from map_transactions_st 
                                    where year = {selected_year} and quarterly =  {selected_quarter}
                                    ORDER BY district ASC'''
    query_df_2 = pd.read_sql_query(query_2, ppdb)

    query_df_2['District'] = query_df_2['District'].str.replace(' district', '')
    query_df_2['id'] = query_df_2['District'].apply(lambda x: dts[x] if x in dts.keys() else '')
    query_df_2['All_Transcations_scale'] = np.log10(query_df_2['All_Transcations'])
    query_df_2['Total_Transaction_Value'] = query_df_2['Total_Transaction_Value'].apply(
        lambda x: f"{round(x / (10 ** 6), 4)} M")
    fig_2 = px.choropleth_mapbox(data_frame=query_df_2,
                                 locations='id',
                                 geojson=ind_dt,
                                 color='All_Transcations_scale',
                                 color_continuous_scale='Jet',
                                 hover_name='District',
                                 hover_data=['All_Transcations', 'Total_Transaction_Value',
                                             'Average_Transaction_Value'],
                                 mapbox_style='carto-positron',
                                 center={'lat': 24, 'lon': 78},
                                 zoom=3)

    # 'Give geo visualisation of state wise users'
    query_3 = f'''select state,
                            registeredUsers,
                            appOpens
                            from map_users 
                            where year = {selected_year} and quarterly =  {selected_quarter}
                            ORDER BY state ASC'''
    query_df_3 = pd.read_sql_query(query_3, ppdb)
    # combing query_df with Id_df
    df_combined_3 = query_df_3.join(id_df)

    # Create the choropleth mapbox
    fig_3 = px.choropleth_mapbox(data_frame=df_combined_3,
                                 locations='id',
                                 geojson=new_indian_states,
                                 color='registeredUsers',
                                 color_continuous_scale='PiYG',
                                 hover_name='state',
                                 hover_data=['appOpens'],
                                 mapbox_style='carto-positron',
                                 center={'lat': 24, 'lon': 78},
                                 zoom=3)
    # Users map district wise
    query_4 = f'''select district, 
                            registeredUsers, 
                            appOpens
                            from map_users_states 
                            where year = {selected_year} and quarterly =  {selected_quarter}
                            ORDER BY district ASC'''
    query_df_4 = pd.read_sql_query(query_4, ppdb)
    query_df_4['district'] = query_df_4['district'].str.replace(' district', '')
    query_df_4['id'] = query_df_4['district'].apply(lambda x: dts[x] if x in dts.keys() else '')
    query_df_4['registeredUsers_scale'] = np.log10(query_df_4['registeredUsers'])
    fig_4 = px.choropleth_mapbox(data_frame=query_df_4,
                                 locations='id',
                                 geojson=ind_dt,
                                 color='registeredUsers_scale',
                                 color_continuous_scale='Jet',
                                 hover_name='district',
                                 hover_data=['registeredUsers', 'appOpens'],
                                 mapbox_style='carto-positron',
                                 center={'lat': 24, 'lon': 78},
                                 zoom=3)
    st.session_state.f_1 = fig_1
    st.session_state.f_2 = fig_2
    st.session_state.f_3 = fig_3
    st.session_state.f_4 = fig_4
    return st.session_state.f_1, st.session_state.f_2, st.session_state.f_3, st.session_state.f_4


# Data Visualisation
def sql_query(chosen_query):
    if chosen_query == 'Give the top states transaction details for 2021 Q1':
        query = f'''select state, count from top_trs_in_states
                   where year = 2021 and quarterly =  1 '''
        query_df = pd.read_sql_query(query, ppdb)
        st.dataframe(query_df)

    elif chosen_query == 'Give the top districts transaction details for 2018 Q4':
        query = f'''select district, count from top_trs_in_districts 
                   where year = 2018 and quarterly =  4 '''
        query_df = pd.read_sql_query(query, ppdb)
        st.dataframe(query_df)

    elif chosen_query == 'Give the top pincodes transaction details for 2022 Q3':
        query = f'''select pincode, count from top_trs_in_pincodes
                   where year = 2022 and quarterly =  3 '''
        query_df = pd.read_sql_query(query, ppdb)
        st.dataframe(query_df)

    elif chosen_query == 'Give the aggregate categorical transaction for 2019 Q2':
        query = f'''select payment, payment_count from aggregated_transaction
                   where year = 2019 and quarterly =  2 '''
        query_df = pd.read_sql_query(query, ppdb)
        fig = px.bar(query_df, x='payment', y='payment_count', color='payment')
        st.plotly_chart(fig)

    elif chosen_query == 'plot pie chat for the transactions of Delhi for 2022 Q4':
        query = f'''select payment, total_amount from aggregated_transaction_states
                   where year = 2022 and quarterly =  4 '''
        query_df = pd.read_sql_query(query, ppdb)
        # Create a Pie Chart
        fig = go.Figure(data=[go.Pie(labels=query_df['payment'], values=query_df['total_amount'])])

        # Set the chart title
        fig.update_layout(
            title='Pie Chart for the transactions of Delhi for the Q4 of the year 2022')

        st.plotly_chart(fig)

    elif chosen_query == 'Give the line chart for the transaction of India for all given years':
        query = '''select year, SUM(total_amount) as transaction
                   from aggregated_transaction
                   GROUP BY year'''
        query_df = pd.read_sql_query(query, ppdb)
        fig = px.line(query_df, x='year', y='transaction', labels={'x': 'YEAR', 'y': 'TRANSACTION AMOUNT'})
        fig.update_traces(mode='markers+lines', text=query_df['transaction'], textposition='top center',
                          marker_color='red')
        st.plotly_chart(fig)
    elif chosen_query == 'Give the bar chart for the Users count in India for all given years':
        query = '''select year, SUM(count) as users
                   from aggregated_users
                   GROUP BY year'''
        query_df = pd.read_sql_query(query, ppdb)
        fig = px.bar(query_df, x='year', y='users', color='users',
                     labels={'x': 'YEAR', 'y': 'USERS'})
        st.plotly_chart(fig)
    elif chosen_query == 'Give the bar chart for the Users of different devices in India for all given years':
        query = '''select year, brand, count as users
                   from aggregated_users'''
        query_df = pd.read_sql_query(query, ppdb)
        fig = px.bar(query_df, x='year', y='users', color='brand', barmode='group')
        st.plotly_chart(fig)
    elif chosen_query == 'Scatter plot for the transaction of states in 2020-Q2':
        query = '''select state, count, amount
                   from map_transactions
                   where year = 2020 and quarterly = 2'''
        query_df = pd.read_sql_query(query, ppdb)
        fig = px.scatter(query_df, x='count', y='amount', color='state', size='amount')
        st.plotly_chart(fig)
    elif chosen_query == 'Scatter plot for payment type in India in 2021':
        query = '''select payment, SUM(payment_count) as payment_count, 
                    SUM(total_amount) as total_amount
                    from aggregated_transaction
                    where year = 2021
                    GROUP BY payment'''
        query_df = pd.read_sql_query(query, ppdb)
        fig = px.scatter(query_df, x='payment_count', y='total_amount', color='payment', size='total_amount')
        st.plotly_chart(fig)
    elif chosen_query == 'Scatter plot for district transation for all given years':
        query = '''select district, SUM(count) as payment_count, 
                            SUM(amount) as total_amount
                            from map_transactions_st
                            GROUP BY district'''
        query_df = pd.read_sql_query(query, ppdb)
        fig = px.scatter(query_df, x='payment_count', y='total_amount', color='district', size='total_amount')
        st.plotly_chart(fig)
    elif chosen_query == 'Scatter plot for district transation in UP for all given years':
        query = '''select district, SUM(count) as payment_count, 
                                    SUM(amount) as total_amount
                                    from map_transactions_st
                                    WHERE state = 'uttar-pradesh'
                                    GROUP BY district'''
        query_df = pd.read_sql_query(query, ppdb)
        fig = px.scatter(query_df, x='payment_count', y='total_amount', color='district', size='total_amount')
        st.plotly_chart(fig)
    elif chosen_query == 'Bar chart of states transactions in 2022':
        query = '''select state, SUM(amount) as total_transaction
                   from map_transactions
                   WHERE year=2022
                   GROUP BY state'''
        query_df = pd.read_sql_query(query, ppdb)
        fig = px.bar(query_df, x='state', y='total_transaction', color='state',
                     barmode='group')
        # Adjusting the bar thickness
        fig.update_traces(width=1)
        st.plotly_chart(fig)


# STREAMLIT interactive
def main():
    # Enable wide mode
    st.set_page_config(page_title='PhonePe Pulse Data Visualisation', layout="wide")
    st.header("Phonepe Pulse Data Visualization and Exploration")
    if st.button("Create dataframes and push to SQL database"):
        df_sql()
        st.write('Dataframes created and pushed to SQL database')

    cursor.execute('select * from top_st_pin_users')
    if cursor.fetchall():
        selected_option = st.selectbox('Select a Year and Quarterly',
                                       ['Select an option', '2018 Q1', '2018 Q2', '2018 Q3', '2018 Q4',
                                        '2019 Q1', '2019 Q2', '2019 Q3', '2019 Q4',
                                        '2020 Q1', '2020 Q2', '2020 Q3', '2020 Q4',
                                        '2021 Q1', '2021 Q2', '2021 Q3', '2021 Q4',
                                        '2022 Q1', '2022 Q2', '2022 Q3', '2022 Q4'], index=0)

        if st.button('Get Geo-Visualization'):
            if selected_option != 'Select an option':
                select_year = int(selected_option[:4])
                select_quarter = int(selected_option[-1])
                f_1, f_2, f_3, f_4 = expensive_computation(select_year, select_quarter)
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown('Transactions state-wise')
                    st.plotly_chart(f_1)
                with c2:
                    st.markdown('Transactions district-wise')
                    st.plotly_chart(f_2)
                c3, c4 = st.columns(2)
                with c3:
                    st.markdown('Users state-wise')
                    st.plotly_chart(f_3)
                with c4:
                    st.markdown('Users district-wise')
                    st.plotly_chart(f_4)
            else:
                st.write('Select an option and then try again !!!')

        query_list = ['Give the top states transaction details for 2021 Q1',
                      'Give the top districts transaction details for 2018 Q4',
                      'Give the top pincodes transaction details for 2022 Q3',
                      'Give the aggregate categorical transaction for 2019 Q2',
                      'plot pie chat for the transactions of Delhi for 2022 Q4',
                      'Give the line chart for the transaction of India for all given years',
                      'Give the bar chart for the Users count in India for all given years',
                      'Give the bar chart for the Users of different devices in India for all given years',
                      'Scatter plot for the transaction of states in 2020-Q2',
                      'Scatter plot for payment type in India in 2021',
                      'Scatter plot for district transation for all given years',
                      'Scatter plot for district transation in UP for all given years',
                      'Bar chart of states transactions in 2022']
        selected_query = st.selectbox('Select an option', query_list)
        if st.button("Get Report"):
            if selected_query:
                sql_query(selected_query)
            else:
                st.write('Please select the options from all the dropdown boxes and click Get Report')

        if st.button('Clear sql tables'):
            cursor.execute('delete from aggregated_transaction')
            ppdb.commit()
            cursor.execute('delete from aggregated_transaction_states')
            ppdb.commit()
            cursor.execute('delete from aggregated_users')
            ppdb.commit()
            cursor.execute('delete from aggregated_users_state')
            ppdb.commit()
            cursor.execute('delete from map_transactions')
            ppdb.commit()
            cursor.execute('delete from map_transactions_st')
            ppdb.commit()
            cursor.execute('delete from map_users')
            ppdb.commit()
            cursor.execute('delete from map_users_states')
            ppdb.commit()
            cursor.execute('delete from top_trs_in_states')
            ppdb.commit()
            cursor.execute('delete from top_trs_in_districts')
            ppdb.commit()
            cursor.execute('delete from top_trs_in_pincodes')
            ppdb.commit()
            cursor.execute('delete from top_in_st_users')
            ppdb.commit()
            cursor.execute('delete from top_in_dt_users')
            ppdb.commit()
            cursor.execute('delete from top_trs_st_districts')
            ppdb.commit()
            cursor.execute('delete from top_trs_st_pincodes')
            ppdb.commit()
            cursor.execute('delete from top_in_pin_users')
            ppdb.commit()
            cursor.execute('delete from top_st_dt_users')
            ppdb.commit()
            cursor.execute('delete from top_st_pin_users')
            ppdb.commit()
            st.write('SQL Tables Cleared')


if __name__ == '__main__':
    main()
