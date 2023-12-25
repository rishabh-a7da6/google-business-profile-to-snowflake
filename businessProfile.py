# Necessary Imports
import pytz
import json
import logging
import requests
import datetime
import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit
from typing import List
from typing import Tuple

# Log filename
log_file = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}.log"

# Create a custom formatter without milliseconds
date_format = '%Y-%m-%d %H:%M:%S'

# Logging config
logging.basicConfig(filename=log_file, 
                    filemode='a',
                    level=logging.DEBUG,
                    format='%(levelname)s : %(asctime)s : %(message)s',
                    datefmt=date_format)

def getSnowflakeSession(credentials:dict) -> snowpark.Session:
    """
    Create a Snowflake session using the provided credentials and return the session object.

    Parameters:
    - credentials (dict): A dictionary containing Snowflake connection credentials, including
      server, user, password, warehouse, database, schema, and role.

    Returns:
    - snowpark.Session: A Snowflake session object.

    This function configures and creates a Snowflake session using the specified credentials.
    If the session is created successfully, an informational message is logged, and the session
    object is returned. In case of an error during session creation, a critical error is logged,
    and an exception is raised.

    Example:
        # Define Snowflake credentials
        snowflake_credentials = {
            'server': 'your_server',
            'user': 'your_user',
            'password': 'your_password',
            'warehouse': 'your_warehouse',
            'database': 'your_database',
            'schema': 'your_schema',
            'role': 'your_role'
        }

        # Create a Snowflake session\n
        snowflake_session = getSnowflakeSession(snowflake_credentials)
    """
    try:
        s = Session.builder.configs(credentials).create()
        logging.info('Snowflake Session Created Successfully.')
        return s
    
    except Exception as e:
        logging.critical(e)
        raise Exception('Snowflake Connection error : Aborting.')

def convertListToSnowparkDataframe(session:snowpark.Session, data:list) -> snowpark.DataFrame:
    """
    Converts a Python list to snowpark dataframe and returns it.

    Parameters:
    - session (snowpark.Session): A snowpark session Object.
    - data (list): A python list that contains data.

    Retruns:
    - snowpark.Dataframe: A Snowpark Dataframe object.

    This functions takes snowpark session and a data list as argument.
    If dataframe isn't created successfully, an error messaged in logged. But if dataframe creation
    is successful then it returns that dataframe.

    Example
    # Sample data in Python list format
    sample_data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 27},
        {"id": 4, "name": "David", "age": 29}
    ]

    # Convert the Python list to a Snowpark DataFrame\n
    df = convertListToSnowparkDataframe(session, sample_data)
    """
    try:
        return session.createDataFrame(data)
    
    except Exception as e:
        logging.error('Error occured while creating snowpark dataframe')
        logging.error(e)
        return None

def checkAccessTokenExpiaryTime(access_token:str) -> int:
    """
    Checks the expiration time of an access token by querying the Google OAuth2 token information endpoint.

    Parameters:
    - access_token (str): A string representing the access token to be checked.

    Returns:
    - int: The expiration time of the access token in seconds.

    This function queries the Google OAuth2 token information endpoint to retrieve
    details about the provided access token. It then extracts the 'expires_in' field
    from the response JSON to determine the token's expiration time in seconds.

    If the access_token parameter is an empty string, a warning is logged indicating
    that no access token was received for checking.

    If any error occurs during the process of checking the token expiration time, an
    error message is logged, and an empty string is returned.

    Example:
    --------
    # Check the expiration time of an access token
    access_token = "your_access_token_here"
    expiration_time = checkAccessTokenExpiaryTime(access_token)
    if expiration_time:
        print(f"The access token expires in {expiration_time} seconds.")
    else:
        print("Failed to retrieve the expiration time for the access token.")
    """
    try:
        if len(access_token) != 0:
            authURL = 'https://www.googleapis.com/oauth2/v1/tokeninfo'

            URL = f"{authURL}?access_token={access_token}"
            PAYLOAD = {}
            HEADERS = {}

            response = requests.get(url=URL, headers=HEADERS, data=PAYLOAD)

            return int(response.json()['expires_in'])
        
        else:
            logging.warning('No Access token recieved to check its time.')

    except Exception as e:
        logging.error('Error has occured while checking time expiration for access token. Aborting')
        logging.error(e)
        return ""

def getNewAccessToken(credentials:dict) -> str:
    """
    Generates a new access token using Google OAuth2 refresh token.

    Parameters:
    - credentials (dict): A dictionary containing Google OAuth2 credentials:
        - 'client_id' (str): The client ID obtained from Google API Console.
        - 'client_secret' (str): The client secret obtained from Google API Console.
        - 'refresh_token' (str): The refresh token associated with the user's account.

    Returns:
    - str: The new access token generated using the provided refresh token.

    This function uses the Google OAuth2 refresh token to obtain a new access token
    by making a POST request to the Google token endpoint. It requires 'client_id',
    'client_secret', and 'refresh_token' fields within the credentials dictionary.

    If successful, the function returns the newly generated access token.

    If an error occurs during the process of generating the access token, an error
    message is logged, and an empty string is returned.

    Example:
    --------
    # Google OAuth2 credentials
    google_credentials = {
        'client_id': 'your_client_id_here',
        'client_secret': 'your_client_secret_here',
        'refresh_token': 'your_refresh_token_here'
    }

    # Generate a new access token
    new_access_token = getNewAccessToken(google_credentials)
    if new_access_token:
        print(f"New access token generated: {new_access_token}")
    else:
        print("Failed to generate a new access token.")
    """
    try:
        tokenURL = 'https://accounts.google.com/o/oauth2/token'
        clientId = credentials['client_id']
        clientSecret = credentials['client_secret']
        refreshToken = credentials['refresh_token']

        URL = f"{tokenURL}?client_id={clientId}&client_secret={clientSecret}&refresh_token={refreshToken}&grant_type=refresh_token"
        PAYLOAD = {}
        HEADERS = {}

        response = requests.post(url=URL, headers=HEADERS, data=PAYLOAD)

        return response.json()['access_token']
    
    except Exception as e:
        logging.error('Error has occured while generating access token. Aborting')
        logging.error(e)
        return ""

def listAllAccounts(credentials:dict, access_token:str) -> Tuple[list, str]:
    """
    Retrieves a list of all Google My Business accounts associated with the provided credentials.

    Parameters:
    - credentials (dict): A dictionary containing Google OAuth2 credentials:
        - 'client_id' (str): The client ID obtained from Google API Console.
        - 'client_secret' (str): The client secret obtained from Google API Console.
        - 'refresh_token' (str): The refresh token associated with the user's account.
    - access_token (str): The access token used for authentication.

    Returns:
    - Tuple[list, str]: A tuple containing:
        - list: A list of dictionaries representing Google My Business accounts.
        - str: The updated access token used for the API request.

    This function retrieves a list of all Google My Business accounts associated with the provided
    credentials. It utilizes the access token for authorization and iterates through multiple pages
    of accounts if pagination is required.

    If the provided access token is expired or about to expire (less than 300 seconds left), it
    generates a new access token using the provided credentials.

    The function returns a tuple containing the list of accounts and the updated access token to be
    used for subsequent API requests.

    Example:
    --------
    # Google OAuth2 credentials
    google_credentials = {
        'client_id': 'your_client_id_here',
        'client_secret': 'your_client_secret_here',
        'refresh_token': 'your_refresh_token_here'
    }

    # Access token
    access_token = "your_access_token_here"

    # Retrieve a list of all Google My Business accounts
    all_accounts, updated_access_token = listAllAccounts(google_credentials, access_token)
    if all_accounts:
        print("List of Google My Business accounts:")
        for account in all_accounts:
            print(account)
    else:
        print("Failed to retrieve the list of accounts.")
    """
    accounts = list()
    pageToken = ''

    while True:

        if access_token is None or checkAccessTokenExpiaryTime(access_token) < 300:
            access_token = getNewAccessToken(credentials=credentials)
    
        URL = f'https://mybusinessaccountmanagement.googleapis.com/v1/accounts?pageToken={pageToken}'
        PAYLOAD = {}
        HEADERS = {
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.get(url=URL, headers=HEADERS, data=PAYLOAD)

        accounts += response.json()['accounts']

        if 'nextPageToken' in response.json():
            pageToken = response.json()['nextPageToken']
        
        else:
            break

    return accounts, access_token

def getAccountLocations(credentials:dict, access_token:str, accountId:str, readMask:str) -> Tuple[list, str]:
    """
    Retrieves a list of locations associated with a specific Google My Business account.

    Parameters:
    - credentials (dict): A dictionary containing Google OAuth2 credentials:
        - 'client_id' (str): The client ID obtained from Google API Console.
        - 'client_secret' (str): The client secret obtained from Google API Console.
        - 'refresh_token' (str): The refresh token associated with the user's account.
    - access_token (str): The access token used for authentication.
    - accountId (str): The ID of the Google My Business account.
    - readMask (str): The read mask specifying which fields to include in the response.

    Returns:
    - Tuple[list, str]: A tuple containing:
        - list: A list of dictionaries representing locations associated with the account.
        - str: The updated access token used for the API request.

    This function retrieves a list of locations associated with a specific Google My Business
    account identified by accountId. It uses the provided access token for authentication and
    iterates through multiple pages of locations if pagination is required.

    If the provided access token is expired or about to expire (less than 300 seconds left), it
    generates a new access token using the provided credentials.

    The function returns a tuple containing the list of locations and the updated access token to
    be used for subsequent API requests.

    Example:
    --------
    # Google OAuth2 credentials
    google_credentials = {
        'client_id': 'your_client_id_here',
        'client_secret': 'your_client_secret_here',
        'refresh_token': 'your_refresh_token_here'
    }

    # Access token and Account ID
    access_token = "your_access_token_here"
    account_id = "your_account_id_here"

    # Read mask specifying fields to include in the response
    read_mask = "field1,field2,field3"

    # Retrieve a list of locations associated with the account
    locations, updated_access_token = getAccountLocations(google_credentials, access_token, account_id, read_mask)
    if locations:
        print("List of locations associated with the account:")
        for location in locations:
            print(location)
    else:
        print("Failed to retrieve the list of locations.")
    """
    allLocations = list()
    pageToken = ''

    while True:

        if access_token is None or checkAccessTokenExpiaryTime(access_token) < 300:
                access_token = getNewAccessToken(credentials=credentials)
                
        URL = f'https://mybusinessbusinessinformation.googleapis.com/v1/accounts/{accountId}/locations?readMask={readMask}&pageToken={pageToken}'
        PAYLOAD = {}
        HEADERS = {
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.get(url=URL, headers=HEADERS, data=PAYLOAD)

        if 'locations' in response.json():
            allLocations += response.json()['locations']

        if 'nextPageToken' in response.json():
            pageToken = response.json()['nextPageToken']
            
        else:
            break

    for i in allLocations:
        i['accountId'] = f'accounts/{accountId}'

    return allLocations, access_token

def fetchMultiDailyMetricsTimeSeries(credentials:dict, access_token:str, locationId:str, dailyMetrics:list, startDate:str, endDate:str) -> Tuple[list, str]:
    """
    Fetches multiple daily metrics time series for a specific location within a date range.

    Parameters:
    - credentials (dict): A dictionary containing Google OAuth2 credentials:
        - 'client_id' (str): The client ID obtained from Google API Console.
        - 'client_secret' (str): The client secret obtained from Google API Console.
        - 'refresh_token' (str): The refresh token associated with the user's account.
    - access_token (str): The access token used for authentication.
    - locationId (str): The ID of the Google My Business location.
    - dailyMetrics (list): A list of strings representing daily metrics to fetch.
    - startDate (str): The start date for the time series data in 'YYYY-MM-DD' format.
    - endDate (str): The end date for the time series data in 'YYYY-MM-DD' format.

    Returns:
    - Tuple[list, str]: A tuple containing:
        - list: A list of dictionaries representing daily metrics time series data.
        - str: The updated access token used for the API request.

    This function retrieves multiple daily metrics time series data for a specific Google My Business
    location identified by locationId within the specified date range. It uses the provided access token
    for authentication and constructs a request to fetch the metrics data.

    If the provided access token is expired or about to expire (less than 300 seconds left), it generates
    a new access token using the provided credentials.

    The function returns a tuple containing the list of dictionaries representing the fetched daily metrics
    time series data and the updated access token to be used for subsequent API requests.

    Example:
    --------
    # Google OAuth2 credentials
    google_credentials = {
        'client_id': 'your_client_id_here',
        'client_secret': 'your_client_secret_here',
        'refresh_token': 'your_refresh_token_here'
    }

    # Access token and Location ID
    access_token = "your_access_token_here"
    location_id = "your_location_id_here"

    # Daily metrics to fetch
    daily_metrics_to_fetch = ['metric1', 'metric2', 'metric3']

    # Start and End dates for the time series data
    start_date = "YYYY-MM-DD"
    end_date = "YYYY-MM-DD"

    # Retrieve multiple daily metrics time series data for the location within the date range
    metrics_data, updated_access_token = fetchMultiDailyMetricsTimeSeries(google_credentials, access_token,
                                                                         location_id, daily_metrics_to_fetch,
                                                                         start_date, end_date)
    if metrics_data:
        print("Fetched daily metrics time series data:")
        for data in metrics_data:
            print(data)
    else:
        print("Failed to fetch daily metrics time series data.")
    """
    # Main list
    result_list = []

    # Creating Metrics string
    metrics = '&'.join([f'dailyMetrics={metric}' for metric in dailyMetrics])

    # Splitting the start and end dates
    start_year, start_month, start_day = startDate.split('-')
    end_year, end_month, end_day = endDate.split('-')

    dailyRange = (
        f"dailyRange.start_date.year={start_year}&"
        f"dailyRange.start_date.month={int(start_month)}&"
        f"dailyRange.start_date.day={int(start_day)}&"
        f"dailyRange.end_date.year={end_year}&"
        f"dailyRange.end_date.month={int(end_month)}&"
        f"dailyRange.end_date.day={int(end_day)}"
    )


    if access_token is None or checkAccessTokenExpiaryTime(access_token) < 300:
        access_token = getNewAccessToken(credentials=credentials)

    URL = f'https://businessprofileperformance.googleapis.com/v1/locations/{locationId}:fetchMultiDailyMetricsTimeSeries?{metrics}&{dailyRange}'
    PAYLOAD = {}
    HEADERS = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url=URL, headers=HEADERS, data=PAYLOAD)

    data = response.json()

    result_dict = {}

    if 'multiDailyMetricTimeSeries' in data:

        for metric_time_series in data['multiDailyMetricTimeSeries']:
            for daily_metric in metric_time_series['dailyMetricTimeSeries']:
                metric = daily_metric['dailyMetric']
                dated_values = daily_metric['timeSeries']['datedValues']

                # Convert dates to datetime objects and get the minimum date
                dates = [datetime.date(d['date']['year'], d['date']['month'], d['date']['day']) for d in dated_values]
                min_date = min(dates) if dates else None

                for dated_value in dated_values:
                    date = datetime.date(dated_value['date']['year'], dated_value['date']['month'], dated_value['date']['day'])
                    formatted_date = date.strftime("%Y-%m-%d")
                    value = int(dated_value.get('value', 0))

                    # Only append if the date is not None and is greater than or equal to the minimum date found
                    if min_date and date >= min_date:
                        if formatted_date not in result_dict:
                            result_dict[formatted_date] = {'Date': formatted_date}
                        result_dict[formatted_date][metric] = value

        # Convert the dictionary into a list of dictionaries to resemble the DataFrame structure
        result_list += list(result_dict.values())

        for i in result_list:
            i['locationId'] = f'locations/{locationId}'

        return result_list, access_token
    
    else:
        return [], access_token

def snowparkDataframeToSnowflakeTable(snowparkDataframe:snowpark.DataFrame, tableName:str, writeMode:str='overwrite') -> int:
    """
    Copies a Snowpark DataFrame to a Snowflake table.

    Parameters:
    - snowparkDataframe (snowpark.DataFrame): The Snowpark DataFrame to be copied.
    - tableName (str): The name of the Snowflake table to which the DataFrame will be copied.
    - writeMode (str, optional): The write mode for copying data ('overwrite' by default).

    Returns:
    - int: The count of rows in the Snowpark DataFrame.

    This function copies the contents of a Snowpark DataFrame to a Snowflake table. The 'writeMode'
    parameter specifies the behavior in case the table already exists. By default, it overwrites the
    existing table data with the DataFrame contents.

    The function returns the count of rows in the Snowpark DataFrame that was copied to the Snowflake table.

    Example:
    --------
    # Assuming 'snowparkDataFrame' and 'target_table_name' are initialized
    target_table_name = "your_target_table_name_here"
    row_count = snowparkDataframeToSnowflakeTable(snowparkDataFrame, target_table_name)
    if row_count > 0:
        print(f"Successfully copied {row_count} rows to the Snowflake table '{target_table_name}'.")
    else:
        print("Failed to copy the Snowpark DataFrame to the Snowflake table.")
    """
    try:
        snowparkDataframe.write.mode(writeMode).save_as_table(tableName)
        return snowparkDataframe.count()

    except Exception as e:
        logging.error('Error occured while copying snowpark dataframe to table.')
        logging.error(e)
        return 0




