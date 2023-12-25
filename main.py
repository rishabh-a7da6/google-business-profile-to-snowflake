from businessProfile import *

# Reading snowfake Credentials
with open('snowflake.json', 'r') as f:
    snowflake_creds = json.load(f)

# Reading Google Credentials
with open('google-business-creds.json', 'r') as d:
    google_creds = json.load(d)

# Global Variables
accessToken = None
locations_data = list()
metrics_data = list()

# API Parameters
read_mask = 'title,storeCode,name'
requiredMetrics = ['WEBSITE_CLICKS', 'CALL_CLICKS', 'BUSINESS_IMPRESSIONS_DESKTOP_MAPS']
start_date = '2023-06-01'
end_date = '2023-06-10'

# Snowflake Parameters
# Format of table name should be 'DATABASE_NAME.SCHEMA_NAME.TABLE_NAME'
accountsTable = 'RISHABH_DB.GOOGLE_BUSINESS_PROFILE.ACCOUNTS'
locationsTable = 'RISHABH_DB.GOOGLE_BUSINESS_PROFILE.ACCOUNT_LOCATIONS'
metricsTable = 'RISHABH_DB.GOOGLE_BUSINESS_PROFILE.METRICS'

try :
    # Get snowflake session
    snowflake_session = getSnowflakeSession(credentials=snowflake_creds)

    # Get list of accounts in Google Business Profile
    allAccountDetails, accessToken = listAllAccounts(credentials=google_creds, access_token=accessToken)

    # Transfer accounts to snowflake table
    df = convertListToSnowparkDataframe(session=snowflake_session, data=allAccountDetails)

    # adding loadtimestamp in dataframe
    current_timestamp = datetime.datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
    df = df.withColumn('LOADTIMESTAMP', lit(current_timestamp))
    df = df.withColumn('LOADTIMESTAMP', df["LOADTIMESTAMP"].cast('timestamp'))
    
    rows = snowparkDataframeToSnowflakeTable(snowparkDataframe=df, tableName=accountsTable)
    logging.info(f'{rows} number of rows copied in {accountsTable} table.')

    # Iterating over each account id
    for item in allAccountDetails:
        if 'name' in item:
            account_Id = item['name'].split("/")[1]

            # Get list of all locations in a particular account
            allLocationDetails, accessToken = getAccountLocations(credentials=google_creds, access_token=accessToken, accountId=account_Id, readMask=read_mask)

            # saving data for each location in a list
            if allLocationDetails:
                locations_data += allLocationDetails
    
            # Iterating over each location id for each account id
            for item2 in allLocationDetails:
                if 'name' in item2:
                    location_Id = item2['name'].split('/')[1]

                    # Get Daily Metric
                    metricsDataFromAPI, accessToken = fetchMultiDailyMetricsTimeSeries(credentials=google_creds, access_token=accessToken, locationId=location_Id,
                                                                                        dailyMetrics=requiredMetrics,
                                                                                        startDate=start_date, endDate=end_date)
                    
                    # Saving metric data in separate list
                    if metricsDataFromAPI:
                        metrics_data += metricsDataFromAPI

    # Transfer metrics data to snowflake
    if metrics_data:
        df = convertListToSnowparkDataframe(session=snowflake_session, data=metrics_data)

        # adding loadtimestamp in dataframe
        current_timestamp = datetime.datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
        df = df.withColumn('LOADTIMESTAMP', lit(current_timestamp))
        df = df.withColumn('LOADTIMESTAMP', df["LOADTIMESTAMP"].cast('timestamp'))

        rows = snowparkDataframeToSnowflakeTable(snowparkDataframe=df, tableName=metricsTable)
        logging.info(f'{rows} number of rows copied in {metricsTable} table.')

    # Transfer locations data to snowflake
    if locations_data:
        df = convertListToSnowparkDataframe(session=snowflake_session, data=locations_data)

        # adding loadtimestamp in dataframe
        current_timestamp = datetime.datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
        df = df.withColumn('LOADTIMESTAMP', lit(current_timestamp))
        df = df.withColumn('LOADTIMESTAMP', df["LOADTIMESTAMP"].cast('timestamp'))

        rows = snowparkDataframeToSnowflakeTable(snowparkDataframe=df, tableName=locationsTable)
        logging.info(f'{rows} number of rows copied in {locationsTable} table.')


except Exception as e:
    logging.error('Something unexpected occured in main file.')
    logging.error(e)