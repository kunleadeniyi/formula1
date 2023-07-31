import requests
import json
import pandas as pd
import datetime
import re
import numpy as np
import logging

from sqlalchemy import MetaData, Table

from db import get_db_connection

current_year = datetime.date.today().year

base_url = "http://ergast.com/api/f1"

db_engine =  get_db_connection()


# # seasons = requests.get(
# #     url=f"http://ergast.com/api/f1/drivers/alonso/constructors/renault/seasons"
# # )

# params = {
#     'limit': 5
# }

# # seasons = requests.get(url=f"{base_url}/seasons.json", params=params)
# circuits = requests.get(url=f"{base_url}/circuits.json", params=params)

# circuits = requests.get(url="http://ergast.com/api/f1/constructors.json", params=params)

# helper functions
def convert_to_dict(x):
    if pd.notna(x) and isinstance(x, str):
        out = json.loads(x.replace('\'', '\"'))
    elif pd.notna(x) and isinstance(x, dict):
        out = x
    else:
        out = x
    return out


def convert_to_list(x):
    if pd.notna(x).all() and isinstance(x, str):
        string = x.replace('\'','\"')
        corrected_text = re.sub(r'(?<=[a-zA-Z])\"(?=[a-zA-Z])', '\'', string) # to fix names like O"Brien after replace ' with "
        out = json.loads(corrected_text) 
    elif pd.notna(x).all() and isinstance(x, list):
        corrected_text = re.sub(r'(?<=[a-zA-Z])\"(?=[a-zA-Z])', '\'', str(x)) # to fix names like O"Brien after replace ' with "
        out = x
    else:
        out = x
    return out

def create_dataframe(data: list):
    return pd.DataFrame.from_dict(data)


def write_data_to_db(engine, df: pd.DataFrame, table_name: str):
    conn = None
    try:
        with engine.connect() as conn:
            # query = 'INSERT into switch values (?, ?, ?, ?)'
            df.to_sql(table_name, conn, if_exists='append', index=False, schema='formula1')

    except Exception as e:
        print(f"Database write failed due to {e}")
        print(f"Unable to write to {table_name.upper()} table")

    finally: 
        if conn is not None:
            conn.close()


def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


def update_etl_table(is_dim: bool, is_fact: bool, is_init: bool):
    df = pd.DataFrame()
    current_date = datetime.date.today()
    current_time = datetime.datetime.now().strftime('%H:%M:%S')
    int_timestamp = int(datetime.datetime.now().timestamp())

    columns = ['id','is_dimension_table', 'is_fact_table', 'is_initial_data_load', 'etl_load_date', 'etl_load_time']
    entry = [int_timestamp, is_dim, is_fact, is_init, current_date, current_time]

    # print(dict(zip(columns, entry)))
    df = pd.DataFrame(dict(zip(columns, entry)), index=[0])

    if db_engine:
        write_data_to_db(engine=db_engine, df=df, table_name='data_load')
    else: 
        print("unable to update data_load table")
    # print(df.head())


# get data (api call) functions
# get all drivers 
# make api call to get total number of drivers, then call api again with limit param = total number of drivers
def get_all_drivers():
    try:
        drivers = requests.get(url=f"{base_url}/drivers.json")
        
        if drivers.status_code != 200:
            drivers.raise_for_status()
        
        api_driver_count =  drivers.json()['MRData']['total']

        if int(api_driver_count) > 0:
            params = {'limit': int(api_driver_count)}
        else:
            print('Did not get any driver count information')
            raise requests.RequestException("Exception raised: Found no driver")

        # get entire driver list
        f1_drivers_response = requests.get(url=f"{base_url}/drivers.json", params=params)
        f1_drivers_response.raise_for_status()
        f1_drivers =  f1_drivers_response.json()['MRData']['DriverTable']['Drivers']
        
        if isinstance(f1_drivers, list):
            actual_count = len(f1_drivers)
        else:
            actual_count = 0

    except requests.Timeout:
        print("API call timedout")
    except requests.RequestException as re:
        print(f"API call error: {re}")
    except ValueError as ve:
        print(f"Error in decoding response:\n {ve}")
    
    return api_driver_count, actual_count, f1_drivers


def get_all_teams():
    constructors_endpoint = f"{base_url}/constructors.json"
    try:
        constructors = requests.get(url=constructors_endpoint)

        constructors.raise_for_status()
        api_constructors_count = constructors.json()['MRData']['total']

        if int(api_constructors_count) > 0:
            params = {'limit': api_constructors_count}
        else:
            raise requests.RequestException("Exception raised: Found no team (constructors)")
        
        f1_constructors_response = requests.get(url=constructors_endpoint, params=params)
        f1_constructors_response.raise_for_status()
        f1_constructors = f1_constructors_response.json()['MRData']['ConstructorTable']['Constructors']
        actual_constructors_count = len(f1_constructors)

    except requests.Timeout:
        print("API call timedout")
    except requests.RequestException as re:
        print(f"Error calling constructors api: {re}")
    except Exception as e:
        print(f"An exception occured {e}")

    return api_constructors_count, actual_constructors_count, f1_constructors 
    

def get_all_circuits():
    circuits_endpoint = f"{base_url}/circuits.json"
    try:
        circuits = requests.get(url=circuits_endpoint)

        circuits.raise_for_status()
        api_circuits_count = circuits.json()['MRData']['total']

        if int(api_circuits_count) > 0:
            params = {'limit': api_circuits_count}
        else:
            raise requests.RequestException("Exception raised: Found no driver")
        
        f1_circuits_response = requests.get(url=circuits_endpoint, params=params)
        f1_circuits_response.raise_for_status()
        f1_circuits = f1_circuits_response.json()['MRData']['CircuitTable']['Circuits']
        actual_circuits_count = len(f1_circuits)

    except requests.Timeout:
        print("API call timedout")
    except requests.RequestException as re:
        print(f"Error calling circuits api: {re}")
    except Exception as e:
        print(f"An exception occured {e}")

    return api_circuits_count, actual_circuits_count, f1_circuits


# refactor to handle if dimension has more than 1000 observations
def get_dimension(dimension):
    endpoint = f"{base_url}/{dimension.lower()}s.json"
    print(endpoint)
    try:
        dim_response = requests.get(url=endpoint)

        dim_response.raise_for_status()
        api_dim_count = dim_response.json()['MRData']['total']

        if int(api_dim_count) > 0:
            params = {'limit': api_dim_count}
        else:
            raise requests.RequestException(f"Exception raised: Found no {dimension}")
        
        f1_dim_response = requests.get(url=endpoint, params=params)
        f1_dim_response.raise_for_status()
        f1_dim = f1_dim_response.json()['MRData'][f"{dimension}Table"][f"{dimension}s"]
        actual_dim_count = len(f1_dim)

    except requests.Timeout:
        print("API call timedout")
    except requests.RequestException as re:
        print(f"Error calling circuits api: {re}")
    except Exception as e:
        print(f"An exception occured {e}")

    return api_dim_count, actual_dim_count, f1_dim


def get_race_schedule_per_season(season: int):
    endpoint = f"{base_url}/{str(season)}.json"
    # print(endpoint)
    try:
        race_response = requests.get(url=endpoint)
        # print(race_response.status_code)

        race_response.raise_for_status()

        # print(race_response.json()["MRData"])

        f1_races = race_response.json()["MRData"]["RaceTable"]["Races"]
    
    except requests.Timeout:
        print("API call timedout")
    except requests.RequestException as re:
        print(f"Error calling circuits api: {re}")
    except Exception as e:
        print(e)

    return f1_races


def get_race_result(season: int, round: int):
    endpoint = f"{base_url}/{str(season)}/{str(round)}/results.json"
    print(endpoint)
    try:
        race_response = requests.get(url=endpoint)
        # print(race_response.status_code)

        race_response.raise_for_status()

        # print(race_response.json()["MRData"])

        f1_races_results = race_response.json()["MRData"]["RaceTable"]["Races"]
    
    except requests.Timeout:
        print("API call timedout")
    except requests.RequestException as re:
        print(f"Error calling circuits api: {re}")
    except Exception as e:
        print(e)

    return f1_races_results


def get_qualifying_result(season: int, round: int):
    endpoint = f"{base_url}/{str(season)}/{str(round)}/qualifying.json"
    print(endpoint)
    try:
        q_response = requests.get(url=endpoint)
        # print(race_response.status_code)

        q_response.raise_for_status()

        # print(race_response.json()["MRData"])

        f1_races_results = q_response.json()["MRData"]["RaceTable"]["Races"]
    
    except requests.Timeout:
        print("API call timedout")
    except requests.RequestException as re:
        print(f"Error calling circuits api: {re}")
    except Exception as e:
        print(e)

    return f1_races_results


# def historical_schedule():
#     season_count, actual_season_count, f1_seasons = get_dimension('Season')
#     df_seasons = create_dataframe(f1_seasons)

#     years = df_seasons['season'].to_list()
#     years = [eval(year) for year in years]
#     # print(years)

#     # print('yes') if str(current_year) in years else print('no')
#     schedule_df = pd.DataFrame()
#     for year in years:
#         if year != current_year:
#             df_list = get_race_schedule_per_season(year)
#             df = create_dataframe(df_list)
#             schedule_df = pd.concat([schedule_df, df])
    
#     return df_seasons, schedule_df


def clean_circuit_table(df: pd.DataFrame):
    # explode the location column the circuits df
    df["Location"] = df["Location"].apply(lambda x: json.loads(x.replace('\'', '\"')) if isinstance(x, str) else x)
    # df["Location"] = df["Location"].apply(convert_to_dict)

    df_location  = pd.json_normalize(df['Location'])

    df = pd.concat([df.reset_index(drop=True), df_location.reset_index(drop=True)], axis='columns')
    df = df.drop('Location', axis='columns')

    # convert data types
    df['lat'] = df['lat'].astype('float32')
    df['long'] =  df['long'].astype('float32')
    return df

def clean_driver_table(df: pd.DataFrame):
    # df['dateOfBirth'] = pd.to_datetime(df['dateOfBirth'], infer_datetime_format=True)
    df['dateOfBirth'] = pd.to_datetime(df['dateOfBirth'], format="%Y-%m-%d")
    return df


# cleanup functions
def clean_season_table(df: pd.DataFrame):

    # extract circuitId only from the Circuit dictionary 
    df['circuitId'] = df['Circuit'].map(lambda x: json.loads(x.replace('\'', "\""))['circuitId'] if isinstance(x, str) else x['circuitId'])
    # df['circuitId'] = df['Circuit'].map(convert_to_dict)

    df = df.drop(['Circuit'], axis='columns')

    # checking First, second and third and qualifying, time
    # usinf domain knowlegde: races with sprints do not have q3
    df[~df[['FirstPractice', 'SecondPractice', 'ThirdPractice', 'Qualifying']].isna().any(axis='columns')].head()
    df[~df[['FirstPractice']].isna().any(axis='columns')].head()

    to_edit = df[['FirstPractice', 'SecondPractice', 'ThirdPractice', 'Qualifying', 'Sprint']]

    cols = ['FirstPractice', 'SecondPractice', 'ThirdPractice', 'Qualifying', 'Sprint']

    for col in cols:
        to_edit[col] = to_edit[col].map(convert_to_dict)
        # to_edit = pd.concat([to_edit, to_edit[col].apply(pd.Series)], axis='columns')
        to_edit = pd.concat([to_edit.reset_index(drop=True), pd.json_normalize(to_edit[col]).reset_index(drop=True)], axis='columns')

    to_edit = to_edit.drop(cols, axis='columns')
    to_edit.columns = ['fp1_date', 'fp1_time', 'fp2_date', 'fp2_time', 'fp3_date', 'fp3_time', 'qualifying_date', 'qualifying_time', 'sprint_date', 'sprint_time']
    df = df.drop(cols, axis='columns')
    df = pd.concat([df.reset_index(drop=True), to_edit.reset_index(drop=True)], axis='columns')

    df[~df['sprint_date'].isna()].head()
    # convert and clean time related columns
    date_cols = ['date', 'time', 'fp1_date', 'fp1_time', 'fp2_date', 'fp2_time', 'fp3_date', 
                'fp3_time', 'qualifying_date', 'qualifying_time', 'sprint_date', 'sprint_time']

    for col in date_cols:
        if 'date' in col:
            # df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")
        else:
            # df[col] = pd.to_datetime(df[col], format="%H:%M:%SZ").dt.time
            df[col] = pd.to_datetime(df[col], format="%H:%M:%SZ").dt.time
            df[col].replace({np.nan: None}, inplace = True)

    df['season'] = pd.to_numeric(df['season'], errors='coerce').astype('Int64')
    df['round'] = pd.to_numeric(df['round'], errors='coerce').astype('Int64')

    return df


def clean_race_results_table(df: pd.DataFrame):
  
    df = df[['season', 'round', 'url', 'raceName', 'Circuit', 'date', 'Results', 'time']]

    # extract circuitId
    # df['circuitId'] = df['Circuit'].map(lambda x: json.loads(x.replace('\'', "\""))['circuitId'] )
    df['circuitId'] = df['Circuit'].map(lambda x: x.replace('\'', "\"")['circuitId'] if isinstance(x, str) else x['circuitId'])
    df = df.drop(['Circuit'], axis='columns')

    df['Results'] = df['Results'].map(convert_to_list)
    
    # vertically explode results for each round
    df = df.explode('Results')
    df = df.reset_index(drop=True)

    results_explode = pd.json_normalize(df['Results'], sep='_')

    # drop driver details other than driverId 
    # drop constructor details other than constructorId
    new_columns = [column for column in results_explode.columns if not column.startswith(('Driver', 'Constructor')) or column.endswith('Id')]
  
    results_explode = results_explode[new_columns]

    df = pd.concat([df.reset_index(drop=True), results_explode.reset_index(drop=True)], axis='columns')
    df = df.drop(['Results'], axis='columns')

    # convert column to correct data types
    # date - date format, clean up time column
    # number, position, points, grid, laps, FastestLap_rank, FastestLap_lap - integer
    # points, FastestLap_AverageSpeed_speed - float
    # FastestLap_Time_time - time but in minutes

    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
    int_columns =  ['number', 'position', 'grid', 'laps', 'FastestLap_rank', 'FastestLap_lap', 'Time_millis']

    # df[int_columns] = pd.to_numeric(df[int_columns], errors='coerce').astype('Int64') # didnt work, only take series, list
    for c in int_columns:
        # to_numeric convert to float and empty strings to np.nan
        # Int64 allows nullable integers (np.nan) while int64 doesn't
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')

    df['FastestLap_AverageSpeed_speed'] = df['FastestLap_AverageSpeed_speed'].astype('float')
    df['points'] = df['points'].astype('float')
    df['time'] = pd.to_datetime(df['time'], format="%H:%M:%SZ").dt.time

    df['season'] = pd.to_numeric(df['season'], errors='coerce').astype('Int64')
    df['round'] = pd.to_numeric(df['round'], errors='coerce').astype('Int64')

    return df


def clean_qualification_results_table(df: pd.DataFrame):

    df = df[['season', 'round', 'url', 'raceName', 'Circuit', 'date', 'QualifyingResults', 'time']]
    df['circuitId'] = df['Circuit'].map(lambda x: x['circuitId'] if isinstance(x, dict) else json.loads(str(x).replace('\'', '\"'))['circuitId'])

    df = df.drop(['Circuit'], axis='columns')

    # convert_to_list(df['QualifyingResults'][0])
    df['QualifyingResults'] =  df['QualifyingResults'].map(convert_to_list)

    # vertical explode
    df = df.explode('QualifyingResults')
    df = df.reset_index(drop=True)
    
    quali_explode = pd.json_normalize(df['QualifyingResults'], sep='_')

    quali_columns = [c for c in quali_explode.columns if not c.startswith(('Driver', 'Constructor')) or c.endswith('Id')]
    quali_explode = quali_explode[quali_columns]
   
    df = pd.concat([df.reset_index(drop=True), quali_explode.reset_index(drop=True)], axis='columns')
    df = df.drop(['QualifyingResults'], axis='columns')

    # data types
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
    df['time'] = pd.to_datetime(df['time'], format="%H:%M:%SZ").dt.time

    df['number'] = pd.to_numeric(df['number'], errors='coerce').astype('Int64')
    df['position'] = pd.to_numeric(df['position'], errors='coerce').astype('Int64')

    df['Q1'] = pd.to_datetime(df['Q1'], format='%M:%S.%f', errors='coerce').dt.time
    df['Q2'] = pd.to_datetime(df['Q2'], format='%M:%S.%f', errors='coerce').dt.time
    df['Q3'] = pd.to_datetime(df['Q3'], format='%M:%S.%f', errors='coerce').dt.time

    df['season'] = pd.to_numeric(df['season'], errors='coerce').astype('Int64')
    df['round'] = pd.to_numeric(df['round'], errors='coerce').astype('Int64')

    return df

# jprint(circuits.json())
# jprint(circuits.json()['MRData']['total'])


def load_dimension_tables():
    # get circuits
    api_count, actual_count, f1_circuits = get_dimension('Circuit')
    f1_circuits_df = create_dataframe(f1_circuits)
    clean_f1_circuits_df = clean_circuit_table(f1_circuits_df)

    # get constructors
    api_team_count, actual_team_count, f1_teams = get_dimension('Constructor')
    f1_teams_df = create_dataframe(f1_teams)
    clean_f1_teams_df = f1_teams_df

    # get drivers
    api_driver_count, actual_driver_count, f1_drivers = get_dimension('Driver')
    f1_drivers_df = create_dataframe(f1_drivers)
    clean_f1_drivers_df = clean_driver_table(f1_drivers_df)

    if db_engine:
        write_data_to_db(engine=db_engine, df=clean_f1_circuits_df, table_name='circuits')
        write_data_to_db(engine=db_engine, df=clean_f1_teams_df, table_name='constructors')
        write_data_to_db(engine=db_engine, df=clean_f1_drivers_df, table_name='drivers')
        update_etl_table(True, False, False)
    else:
        print("unable to connect to database")


def initial_historical_data_load():

    # get seasons and historical schedule
    season_count, actual_season_count, f1_seasons = get_dimension('Season')
    df_seasons = create_dataframe(f1_seasons)

    years = df_seasons['season'].to_list()
    years = [eval(year) for year in years]
    # print(years)
    # years = [2022]

    historical_f1_schedule_df = pd.DataFrame()
    historical_f1_race_results = pd.DataFrame()
    historical_f1_quali_results = pd.DataFrame()

    for year in years:
    # for year in range(2000,2022):
        if year != current_year:
            temp_season_df = get_race_schedule_per_season(year)
            df = create_dataframe(temp_season_df)
            historical_f1_schedule_df = pd.concat([historical_f1_schedule_df, df])
    
            rounds = historical_f1_schedule_df[historical_f1_schedule_df['season'].astype(int) == year]['round'].to_list()
            rounds = [eval(round) for round in rounds]

            for round in rounds:
                # print(f"round: {round}")
                temp_result_df = get_race_result(year, round)
                df = create_dataframe(temp_result_df)
                historical_f1_race_results = pd.concat([historical_f1_race_results, df])
                
                if year >= 2003: # api only supports qualifying from 2003
                    temp_quali_result_df = get_qualifying_result(year, round)
                    quali_df = create_dataframe(temp_quali_result_df)
                    historical_f1_quali_results = pd.concat([historical_f1_quali_results, quali_df])

    # clean all dfs
    cleaned_historical_f1_schedule_df = clean_season_table(historical_f1_schedule_df)
    cleaned_historical_f1_quali_results = clean_qualification_results_table(historical_f1_quali_results)
    cleaned_historical_f1_race_results = clean_race_results_table(historical_f1_race_results)

    if db_engine:
        write_data_to_db(engine=db_engine, df=cleaned_historical_f1_schedule_df, table_name='seasons')
        update_etl_table(True, True, True) # seasons is kind of a dimensional and fact table (not fully denormalised)

        write_data_to_db(engine=db_engine, df=cleaned_historical_f1_quali_results, table_name='qualification_results')
        write_data_to_db(engine=db_engine, df=cleaned_historical_f1_race_results, table_name='race_results')
        update_etl_table(False, True, True)
    else:
        print("unable to connect to database")


    # historical_f1_race_results.to_csv("historical_f1_race_results.csv")
    # historical_f1_quali_results.to_csv("historical_f1_quali_results.csv")


def update_database(db_table_name, changed_rows, original, engine): # can be used for seasons, quali_results and race_results
  
    metadata = MetaData(schema='formula1')
    table = Table(db_table_name, metadata, autoload_with=engine)

    with engine.connect() as conn:
        with conn.begin():
            for _, row in changed_rows.iterrows():
                stmt = table.update().where(table.c.season == row['season'], table.c.round == row['round'])
                for col in changed_rows.columns:
                    # if col in ['season', 'round']:
                    if str(row[col]) in ['NaT', 'NaN', 'None']:
                        # stmt = stmt.values({col: None})
                        continue
                    # print(col)
                    # print(row[col])
                    # print(original.loc[
                    #         (original['season']==row['season']) & (original['round']==row['round']),
                    #         col].iloc[0])
                    
                    orig = original.loc[
                            (original['season']==row['season']) & (original['round']==row['round']),col
                        ].iloc[0]
                    if ('time' in col) and (str(row[col]) == str(orig)):
                        continue
                    
                    if ('time' in col) and (str(row[col]) != str(orig)):
                        stmt = stmt.values({col: row[col]})
                    
                    if row[col] != orig:
                        stmt = stmt.values({col: row[col]})

                # print(f"PARAMS: {stmt._values}")
                if stmt._values != None:
                    conn.execute(stmt)
                    print(f"season: {row['season']}, round: {row['round']} table: {db_table_name} columns: {stmt._values.keys()} modified")
            
            conn.commit() 
        conn.close()

def load_current_year_schedule():
    
    year = current_year
    f1_schedule_df = pd.DataFrame()
    f1_race_results = pd.DataFrame()
    f1_quali_results = pd.DataFrame()

    #  logic
    #  all ways refresh the season schedule because of postponed races etc
    api_schedule = get_race_schedule_per_season(year)
    api_schedule_df = create_dataframe(api_schedule)
    cleaned_api_schedule_df = clean_season_table(api_schedule_df)

    # upsert schedule
    season_sql = f"select * from formula1.seasons where season = {year}"
    db_season_schedule = pd.read_sql(season_sql, db_engine)
    # print(db_season_schedule)

    merged = cleaned_api_schedule_df.merge(db_season_schedule, on=list(db_season_schedule.columns), how='outer', indicator=True)
    # print(merged)
    # print(merged.dtypes)
    changed = merged[merged['_merge'] == 'left_only'].drop('_merge', axis='columns')
    # left_only includes missing rows that are in the right table 
    # exclude those rows
    exclude = changed[
        ~np.isin(changed[['season', 'round']], merged[merged['_merge'] == 'right_only'][['season', 'round']])
    ]
    changed = changed.drop(exclude.index, axis='index')
    
    # print(changed)
    # print(changed.dtypes)
    
    missing_rows = cleaned_api_schedule_df[
        ~np.isin(
            cleaned_api_schedule_df[['season', 'round']],
            db_season_schedule[['season', 'round']]
        )
    ]

    if db_engine:
        # Assuming changed_rows has been determined
        if db_season_schedule.shape[0] > 0:
            update_database('seasons', changed, db_season_schedule, db_engine)

        write_data_to_db(engine=db_engine, df=missing_rows, table_name='seasons' )


def load_current_year_results():
    pass
#     # new_schedule = cleaned_f1_schedule_df[
#     #     # ~np.isin()
#     # ]

# # >>> df1.set_index('Code', inplace=True)
# # >>> df1.update(df2.set_index('Code'))
# # >>> df1.reset_index()

#     #  get max race result round, check if new race result is available else ignore data load
#     #  do the same for quali

#     temp_rounds = f1_schedule_df[f1_schedule_df['season'].astype(int) == year]['round'].to_list()
#     temp_rounds = [eval(round) for round in temp_rounds]

#     # get inserted rounds
#     season_rounds_sql = f"select * from formula1.seasons where season = {year}"
#     # new_rounds = 

#     for round in temp_rounds:
#         # print(f"round: {round}")
#         temp_result_df = get_race_result(year, round)
#         df = create_dataframe(temp_result_df)
#         f1_race_results = pd.concat([f1_race_results, df])
        
#         temp_quali_result_df = get_qualifying_result(year, round)
#         quali_df = create_dataframe(temp_quali_result_df)
#         f1_quali_results = pd.concat([f1_quali_results, quali_df])

#     # clean all dfs
#     cleaned_f1_schedule_df = clean_season_table(f1_schedule_df)

#     cleaned_f1_schedule_df.to_csv('f1_2023.csv')
#     cleaned_f1_quali_results = clean_qualification_results_table(f1_quali_results)
#     cleaned_f1_race_results = clean_race_results_table(f1_race_results)

#     if db_engine:
#         # check if season is there
#         season_sql = f"select season from formula1.seasons where season = {year} limit 1"
        

#         # write_data_to_db(engine=db_engine, df=cleaned_f1_schedule_df, table_name='seasons')
#         # update_etl_table(True, True, True) # seasons is kind of a dimensional and fact table (not fully denormalised)

#         # write_data_to_db(engine=db_engine, df=cleaned_f1_quali_results, table_name='qualification_results')
#         # write_data_to_db(engine=db_engine, df=cleaned_f1_race_results, table_name='race_results')
#         # update_etl_table(False, True, True)


#         # upsert
#         pass
#     else:
#         print("unable to connect to database")

#     return existing_season_schedule

def main():
    """
    # check database, if initial data has been loaded fetch only data on current season
    # do this by checking the data_load table
    # dim_tables_sql = "select tablename from pg_catalog.pg_tables where schemaname='formula1'"

    # check if dimension tables exist from the pg_catalog
    schema_tables_sql = "select tablename from pg_catalog.pg_tables where schemaname='formula1'"
    schema_tables = pd.read_sql(sql=schema_tables_sql,con=db_engine)

    if 'data_load' not in schema_tables['tablename'].unique():
        print("="*50, "\nNo data Loaded, Starting Initial Load")
        # load all data
        print("\n", "="*50, "\nLoading dimension tables")
        load_dimension_tables()
        print("\n", "="*50, "\nLoading historical results")
        initial_historical_data_load()

        print("all tables loaded")
    else:
        if not set(['seasons', 'circuits', 'constructors', 'drivers']).issubset(set(schema_tables['tablename'].unique())):
            print("="*50, "\nLoading Dimension tables")
            load_dimension_tables()
            print("all dimension tables loaded")

        init_load_sql = "select is_initial_data_load from formula1.data_load where is_initial_data_load = 'true' order by id limit 1"
        is_initial_data_load = pd.read_sql(sql=init_load_sql,con=db_engine)

        if len(is_initial_data_load['is_initial_data_load']) < 1:
            print("="*50, "\nLoading historical results")
            initial_historical_data_load()
            print("all result tables loaded")


    print("all historical and dimension tables already loaded")
    """
    # logic for current season
    load_current_year_schedule()



main()