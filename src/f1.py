import requests
import json
import pandas as pd
import datetime
import re
import logging


current_year = datetime.date.today().year

base_url = "http://ergast.com/api/f1"


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
    return json.loads(x.replace('\'', '\"')) if pd.notna(x) else x

def convert_to_list(x):
    if pd.notna(x):
        string = x.replace('\'','\"')
        corrected_text = re.sub(r'(?<=[a-zA-Z])\"(?=[a-zA-Z])', '\'', string) # to fix names like O"Brien after replace ' with "
        out = json.loads(corrected_text) 
    else:
        out = x
    return out

def create_dataframe(data: list):
    return pd.DataFrame.from_dict(data)

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
    # pass

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


def historical_schedule():
    season_count, actual_season_count, f1_seasons = get_dimension('Season')
    df_seasons = create_dataframe(f1_seasons)

    years = df_seasons['season'].to_list()
    years = [eval(year) for year in years]
    # print(years)

    # print('yes') if str(current_year) in years else print('no')
    schedule_df = pd.DataFrame()
    for year in years:
        if year != current_year:
            df_list = get_race_schedule_per_season(year)
            df = create_dataframe(df_list)
            schedule_df = pd.concat([schedule_df, df])
    
    return df_seasons, schedule_df


def clean_circuit_table(df: pd.DataFrame):
    # explode the location column the circuits df
    df["Location"] = df["Location"].apply(lambda x: json.loads(x.replace('\'', '\"')) if isinstance(x, str) else x)
    dfLocation  = pd.json_normalize(df['Location'])

    df = pd.concat([df.reset_index(drop=True), dfLocation.reset_index(drop=True)], axis='columns')
    df = df.drop('Location', axis='columns')

    # convert data types
    df['lat'] = df['lat'].astype('float32')
    df['long'] =  df['long'].astype('float32')
    return df

def clean_driver_table(df: pd.DataFrame):
    df['dateOfBirth'] = pd.to_datetime(df['dateOfBirth'], infer_datetime_format=True)
    return df


# cleanup functions
def clean_season_table(df):

    # extract circuitId only from the Circuit dictionary 
    df['circuitId'] = df['Circuit'].map(lambda x: json.loads(x.replace('\'', "\""))['circuitId'] )
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
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
        else:
            df[col] = pd.to_datetime(df[col], format="%H:%M:%SZ").dt.time

    return df

def clean_race_results_table(df):
  
    df = df[['season', 'round', 'url', 'raceName', 'Circuit', 'date', 'Results', 'time']]

    # extract circuitId

    df['circuitId'] = df['Circuit'].map(lambda x: json.loads(x.replace('\'', "\""))['circuitId'] )
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

    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    int_columns =  ['number', 'position', 'grid', 'laps', 'FastestLap_rank', 'FastestLap_lap', 'Time_millis']

    # df[int_columns] = pd.to_numeric(df[int_columns], errors='coerce').astype('Int64') # didnt work, only take series, list
    for c in int_columns:
        print(c)
        # to_numeric convert to float and empty strings to np.nan
        # Int64 allows nullable integers (np.nan) while int64 doesn't
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')

    df['FastestLap_AverageSpeed_speed'] = df['FastestLap_AverageSpeed_speed'].astype('float')
    df['points'] = df['points'].astype('float')
    df['time'] = pd.to_datetime(df['time'], format="%H:%M:%SZ").dt.time

    return df

def clean_qualification_results_table(df):

    df = df[['season', 'round', 'url', 'raceName', 'Circuit', 'date', 'QualifyingResults', 'time']]
    df['circuitId'] = df['Circuit'].map(lambda x: json.loads(x.replace('\'', '\"'))['circuitId'])
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
    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    df['time'] = pd.to_datetime(df['time'], format="%H:%M:%SZ").dt.time

    df['number'] = pd.to_numeric(df['number'], errors='coerce').astype('Int64')
    df['position'] = pd.to_numeric(df['position'], errors='coerce').astype('Int64')

    df['Q1'] = pd.to_datetime(df['Q1'], format='%M:%S.%f', errors='coerce').dt.time
    df['Q2'] = pd.to_datetime(df['Q2'], format='%M:%S.%f', errors='coerce').dt.time
    df['Q3'] = pd.to_datetime(df['Q3'], format='%M:%S.%f', errors='coerce').dt.time

    return df

# # api_driver_count, actual_driver_count, f1_drivers = get_all_drivers()
# api_driver_count, actual_driver_count, f1_drivers = get_dimension('Driver')
# f1_drivers_df = create_dataframe(f1_drivers)
# # f1_drivers_df.to_csv("drivers.csv", index=False)
# print("\n\nDriver info")
# print(api_driver_count, actual_driver_count)
# print(f1_drivers_df.head())
# # print(f1_drivers_df.dtypes)

# # api_count, actual_count, f1_c = get_all_circuits()
# api_count, actual_count, f1_c = get_dimension('Circuit')
# df = create_dataframe(f1_c)
# # df.to_csv("circuits.csv", index=False)
# print("\n\nCircuit info")
# print(api_count, actual_count)
# print(df.head())
# # print(df.dtypes)

# api_team_count, actual_team_count, f1_teams = get_dimension('Constructor')
# df_teams = create_dataframe(f1_teams)
# # df_teams.to_csv('constructors.csv', index=False)
# print("\n\nTeam info")
# print(api_team_count, actual_team_count)
# print(df_teams.head())
# # print(df_teams.dtypes)


    # print(df.head())

# schedule_df.to_csv("f1_schedule.csv", index=False)
# df.to_csv('f1_2022.csv')


def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

# print(type(seasons))
# print(seasons.json())

# jprint(circuits.json())
# jprint(circuits.json()['MRData']['total'])

def load_dimension_tables():
    # get circuits
    api_count, actual_count, f1_circuits = get_dimension('Circuit')
    f1_circuits_df = create_dataframe(f1_circuits)
    clean_f1_circuits_df = clean_circuit_table(f1_drivers_df)

    # get constructors
    api_team_count, actual_team_count, f1_teams = get_dimension('Constructor')
    f1_teams_df = create_dataframe(f1_teams)
    clean_f1_teams_df = f1_teams_df

    # get drivers
    api_driver_count, actual_driver_count, f1_drivers = get_dimension('Driver')
    f1_drivers_df = create_dataframe(f1_drivers)
    clean_f1_drivers_df = clean_driver_table(f1_drivers_df)

    pass

def initial_historical_data_load():

    # get seasons and historical schedule
    season_count, actual_season_count, f1_seasons = get_dimension('Season')
    df_seasons = create_dataframe(f1_seasons)

    years = df_seasons['season'].to_list()
    years = [eval(year) for year in years]
    # print(years)

    historical_f1_schedule_df = pd.DataFrame()
    for year in years:
    # for year in range(2000,2022):
        if year != current_year:
            temp_season_df = get_race_schedule_per_season(year)
            df = create_dataframe(temp_season_df)
            historical_f1_schedule_df = pd.concat([historical_f1_schedule_df, df])
    
    # clean historical table (df_schedule)

    historical_f1_race_results = pd.DataFrame()
    historical_f1_quali_results = pd.DataFrame()
    for year in years:
    # for year in range(2000,2022):
        if year != current_year:
            # get all rounds from the historical df
            # print(historical_f1_schedule_df.dtypes)
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



    # historical_f1_race_results.to_csv("historical_f1_race_results.csv")
    historical_f1_quali_results.to_csv("historical_f1_quali_results.csv")

initial_historical_data_load() # create a table called data_load with columns date, initial_load: bool, etl_load_time

def main():

    # check database, if initial data has been loaded fetch only data on current season
    # do this by checking the data_load table
    pass