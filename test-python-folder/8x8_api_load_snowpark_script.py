CREATE OR REPLACE PROCEDURE SP_8x8_API_LOAD()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python','requests')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (eight_x_eight_integration)
AS
$$
import re
import requests
#from typing import Dict, Any
import base64
import json 
import time
from datetime import datetime, timezone,timedelta
import calendar

from snowflake.snowpark import Session

def get_bearer_token(username: str, password: str, auth_endpoint: str) -> str:
    
    #Obtain Bearer token using client credentials flow
    
    try:
        # Create basic auth header for token request
        auth_string = f"{username}:{password}"
        base64_bytes = base64.b64encode(auth_string.encode()).decode()
        auth_header = f"Basic {base64_bytes}"
        
        # Request token
        response = requests.post(
            auth_endpoint,
            headers={
                'Authorization': auth_header,
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={'grant_type': 'client_credentials'}
        )
        
        response.raise_for_status()
        token_response = response.json()
        return token_response['access_token']
        
    except requests.exceptions.RequestException as err:
        print(f"Error obtaining Bearer token: {err}")
        raise

def get_report_id(session,url:str,headers:dict,Start_Time,End_Time,Report_Type,Data_Source_Name,Report_Granularity,Report_Group_By,Table_Name,Water_Mark_Column)->str:

    if not Start_Time: 
        max_start_time_row= session.sql(f'SELECT MAX({Water_Mark_Column}) AS TIME FROM {Table_Name}').collect()[0]
        Start_Time= max_start_time_row['TIME']
        Start_Time = Start_Time + timedelta(milliseconds=1)

    if Start_Time.tzinfo is None:
        Start_Time = Start_Time.replace(tzinfo=timezone.utc)
    else:
        Start_Time = Start_Time.astimezone(timezone.utc)

    Start_Time= Start_Time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


    if not End_Time:
        End_Time = datetime.now(timezone.utc)

    if End_Time.tzinfo is None:
        End_Time = End_Time.replace(tzinfo=timezone.utc)
    else:
        End_Time = End_Time.astimezone(timezone.utc)

    
    End_Time = End_Time.replace(minute=0,second=0,microsecond=0)

    End_Time = End_Time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'   
            
    body={
        "type": Report_Type,
        "title": "Test",
        "dateRange": {
            "start": Start_Time,
            "end": End_Time
        },
    }

    if Report_Granularity:
        body['granularity'] = Report_Granularity
    if Report_Group_By:
        body['groupBy']={"name":Report_Group_By}

    response = requests.post(url, headers=headers,json=body)
    data= response.json()
    return data.get('id')
    
def create_table_if_not_exists(session,create_table_sql :str):
    session.sql(create_table_sql).collect()

def flatten_record(record):
    flat = {}
    nested_data = {}
    
    for k, v in record.items():
        if isinstance(v, list):
            for item in v:
                key = item.get("key")
                raw_value = item.get("value")

                # Normalize the value format
                if isinstance(raw_value, dict):
                    # e.g., {"value": 90, "output": false}
                    value = raw_value.get("value")
                elif isinstance(raw_value, list):
                    # Convert list to comma-separated string
                    value = ','.join(str(i) for i in raw_value)
                else:
                    value = raw_value

                if key:
                    nested_data[key] = value
        else:
            flat[k] = v

    # Merge nested values into flat record
    flat.update(nested_data)
    return flat


def process_api_response(api_response):
    # Unwrap if JSON response is a wrapper dict with 'content' array
    if isinstance(api_response, dict) and 'content' in api_response:
        api_response = api_response['content']
    
    # Filter to dict records only, skip non-dict items
    api_response = [r for r in api_response if isinstance(r, dict)]
    
    # Early exit if no valid records
    if not api_response:
        return [], []
    
    simple_keys = set()
    nested_keys_set = set()
    nested_keys = ('metric/items', 'metrics', 'items')

    # First pass: collect all keys (flat + nested)
    for record in api_response:
        for k, v in record.items():
            if k in nested_keys and isinstance(v, list):
                for item in v:
                    key = item.get("key")
                    if key:
                        nested_keys_set.add(key)
            else:
                simple_keys.add(k)

    # Sorted column order for stability
    simple_keys = sorted(simple_keys)
    nested_keys_list = sorted(nested_keys_set)
    columns = simple_keys + nested_keys_list

    # Second pass: build row tuples
    dict_rows = []
    for record in api_response:
        flat = flatten_record(record)
        # Ensure all columns present with None if missing
        row_dict = {camel_to_upper_snake(col): flat.get(col) for col in columns}
        dict_rows.append(row_dict)

    return columns, dict_rows



def process_api_response_old(api_response):
    simple_keys = set()
    nested_keys_set = set()
    nested_keys = ('metric/items', 'metrics', 'items')

    # First pass: collect all keys (flat + nested)
    for record in api_response:
        for k, v in record.items():
            if k in nested_keys and isinstance(v, list):
                for item in v:
                    key = item.get("key")
                    if key:
                        nested_keys_set.add(key)
            else:
                simple_keys.add(k)

    # Sorted column order for stability
    simple_keys = sorted(simple_keys)
    nested_keys_list = sorted(nested_keys_set)
    columns = simple_keys + nested_keys_list

    # Second pass: build row tuples
    dict_rows = []
    for record in api_response:
        flat = flatten_record(record)
        # Ensure all columns present with None if missing
        row_dict = {camel_to_upper_snake(col): flat.get(col) for col in columns}
        dict_rows.append(row_dict)

    return columns, dict_rows

def camel_to_upper_snake(name):
    # Convert camelCase to snake_case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).upper()
    # Replace '.' and '-' with '_'
    s3 = re.sub(r'[.-]', '_', s2)
    return s3
    
def check_report_generation_status(Status_URL, timeout, interval,headers:dict):
    start_time = time.time()
    
    while True:
        status_url = Status_URL
        response = requests.get(status_url,headers= headers)
        data = response.json()
        
        status = data.get('status')
        print(f"Status: {status}")
        
        if status == 'DONE':
            print("Status is done. Exiting.")
            break
        
        elapsed = time.time() - start_time
        if elapsed >= timeout:
            return True
            
        
        time.sleep(interval)

def insert_data(input_data,session,table_schema_array,Table_Name,Write_Mode):
    cols, dict_rows = process_api_response(input_data)

    #return str(dict_rows)
                
    #str(dict_rows)
    df=  session.create_dataframe(dict_rows)
    df=df.select(table_schema_array)
    #return str(df.columns)
    if df.columns != table_schema_array:
        return "DataFrame column order doesn't match target table schema."

    if df.count():
        df.write.save_as_table(
            Table_Name,
            mode=Write_Mode  #  "append" or "overwrite" if needed
        )
        row_count = df.count()


    return row_count

def generate_monthly_date_ranges_old(start_str, end_str):
    """
    Generate monthly date ranges from start_str to end_str.
    Each range is from the first day of the month at 00:00:00.000Z
    to the last day of the month at 23:59:59.999Z.
    """
    start_date = datetime.strptime(start_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_str, '%Y-%m-%d')
    
    date_ranges = []
    current = start_date.replace(day=1)  # go to first day of start month
    
    while current <= end_date:
        year = current.year
        month = current.month
        
        # Get last day of current month
        last_day = calendar.monthrange(year, month)[1]
        
        # Construct start and end datetime strings for the month
        start_dt = current.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_dt = current.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999000)
        
        # Clip end_dt to not exceed end_date
        if end_dt.date() > end_date.date():
            end_dt = end_date.replace(hour=23, minute=59, second=59, microsecond=999000)
        
        date_ranges.append({
            "start": start_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            "end": end_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        })
        
        # Move to first day of next month
        if month == 12:
            current = current.replace(year=year+1, month=1)
        else:
            current = current.replace(month=month+1)
    
    return date_ranges


def generate_monthly_date_ranges(start_str, end_str):
    """
    Generate date ranges between start_str and end_str.

    Rules:
    - If the span is less than one full month, return a single range [start, end].
    - If it spans >= 1 full month:
        * Include full calendar-month slices (00:00:00.000Z of the 1st to 23:59:59.999Z of the last day).
        * Also include a final remainder slice from the first day after the last full month up to end.
    """
    # Parse inputs
    start_date = datetime.strptime(start_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_str, '%Y-%m-%d')

    if end_date < start_date:
        return []

    # Normalized helpers
    def month_start(dt):
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def month_end(dt):
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        return dt.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999000)

    def to_iso_z(dt):
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    # Determine first full-month window that could start
    start_ms = month_start(start_date)
    end_me = month_end(end_date)

    # Check if there exists at least one full calendar month fully contained in [start_date, end_date]
    # A full month exists if the end of the start month is before or equal to end_date,
    # and the start of that month is before or equal to end_date.
    # More robustly, find the first candidate full month:
    # - If start_date is not the first of month 00:00, the first full month starts next month.
    if start_date == start_ms:
        first_full_month = start_ms
    else:
        # move to first day of next month
        y, m = start_date.year, start_date.month
        if m == 12:
            first_full_month = datetime(y + 1, 1, 1)
        else:
            first_full_month = datetime(y, m + 1, 1)

    # The last possible full month is the month of end_date only if end_date covers its last day at end of day.
    # Since end_date is a date (no time), consider that user’s period includes full end day,
    # so the month is full only if end_date is that month’s last day.
    last_day_in_end_month = calendar.monthrange(end_date.year, end_date.month)[1]
    if end_date.day == last_day_in_end_month:
        last_full_month = month_start(end_date)
    else:
        # last full month is the previous month’s start
        y, m = end_date.year, end_date.month
        if m == 1:
            last_full_month = datetime(y - 1, 12, 1)
        else:
            last_full_month = datetime(y, m - 1, 1)

    date_ranges = []

    # Case 1: no full month fits between start and end -> single range
    if first_full_month > last_full_month:
        start_dt = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = end_date.replace(hour=23, minute=59, second=59, microsecond=999000)
        date_ranges.append({
            "start": to_iso_z(start_dt),
            "end": to_iso_z(end_dt),
        })
        return date_ranges

    # Case 2: there are full months. Build:
    #  - Leading partial (from start to end of month before first_full_month), if needed
    if start_date < first_full_month:
        # end at last day of start_date's month or end_date if earlier
        lead_end = month_end(start_date)
        if lead_end.date() > end_date.date():
            lead_end = end_date.replace(hour=23, minute=59, second=59, microsecond=999000)
        start_dt = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        date_ranges.append({
            "start": to_iso_z(start_dt),
            "end": to_iso_z(lead_end),
        })

    #  - Full month slices from first_full_month to last_full_month
    cur = first_full_month
    while cur <= last_full_month:
        start_dt = cur.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = month_end(cur)
        date_ranges.append({
            "start": to_iso_z(start_dt),
            "end": to_iso_z(end_dt),
        })
        # advance to next month
        y, m = cur.year, cur.month
        if m == 12:
            cur = datetime(y + 1, 1, 1)
        else:
            cur = datetime(y, m + 1, 1)

    #  - Trailing remainder slice (from the first day after last_full_month to end), if needed
    # next day after end of last_full_month
    if last_full_month:
        y, m = last_full_month.year, last_full_month.month
        if m == 12:
            after_last_full = datetime(y + 1, 1, 1)
        else:
            after_last_full = datetime(y, m + 1, 1)
        if after_last_full <= end_date:
            start_dt = after_last_full.replace(hour=0, minute=0, second=0, microsecond=0)
            end_dt = end_date.replace(hour=23, minute=59, second=59, microsecond=999000)
            date_ranges.append({
                "start": to_iso_z(start_dt),
                "end": to_iso_z(end_dt),
            })

    return date_ranges



def create_report_and_insert_data(username,password,auth_endpoint,Table_Name, url, Status_URL, table_schema_array, Write_Mode, Create_Report,session,report_url,headers,Start_Time,End_Time,Report_Type, Data_Source_Name,Base_URL,Pagination_Type,Report_Granularity,Report_Group_By,Water_Mark_Column,Page_Start_Index):
    final_result = []
    # Get Bearer token
    bearer_token = get_bearer_token(username, password, auth_endpoint)
    headers = {
                'Authorization':f"Bearer {bearer_token}",
                'Accept': 'application/json'
            }
    
    if Create_Report:
        report_id = get_report_id(session,report_url,headers,Start_Time,End_Time,Report_Type,Data_Source_Name,Report_Granularity,Report_Group_By,Table_Name,Water_Mark_Column) # 
        if not report_id:
            return "no report-id"
        url = url.replace("{report-id}",str(report_id))
        if Status_URL:
            #return Status_URL
            Status_URL = F"{Base_URL}/{Status_URL}"
            Status_URL = Status_URL.replace("{report-id}",str(report_id))
            #return Report_URL
            if check_report_generation_status(Status_URL,60,5,headers):
                return "Error: Time out after 60 seconds"
                                   
                   
    all_data = []

            
    if Pagination_Type=='DOCUMENT_ID':
        while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 401:
                        bearer_token = get_bearer_token(username, password, auth_endpoint)
                        headers = {'Authorization': f"Bearer {bearer_token}", 'Accept': 'application/json'}
                        response = requests.get(url, headers=headers, timeout=90)
            
            data = response.json()
            if response.status_code != 200 or len(data) ==0:
                break
                
            all_data.extend(data) 
            
            link =  response.headers.get('Link')
            if link:
                match = re.search(r'<([^>]+)>;\s*rel="next"', link)
                if match:
                    url= match.group(1)
                else:
                    break
            else:
                break
                        
            
    if Pagination_Type=='PAGE_NO':
        #return 'in page'
        params ={
            "page":Page_Start_Index
        }
        while True:
            #Send GET request
            response = requests.get(url, headers=headers,params= params)
            if response.status_code == 401:
                        bearer_token = get_bearer_token(username, password, auth_endpoint)
                        headers = {'Authorization': f"Bearer {bearer_token}", 'Accept': 'application/json'}
                        response = requests.get(url, headers=headers,params=params, timeout=90)
            str(response)
            data = response.json()
            
            # Extract content array for QM endpoints, pass-through for analytics
            items = data.get('content', data) if isinstance(data, dict) else data



            if response.status_code != 200 or not items:
                break
            all_data.extend(items) 
            params['page'] +=1
                     
    row_count = 0
    if len(all_data):
        row_count = insert_data(all_data,session,table_schema_array,Table_Name,Write_Mode)
    
            
           
    return f"SUCCESS :: {row_count} row(s) added to {Table_Name}"


def main(session: Session):
    # Configuration
    # print("Hello World")
    #return "Hello World"
    username = "eght_YzQxMmU3MzMtYzkxMy00YjZlLTg5ZDUtY2Y1NmIxOGVhZDA3"#API Key
    password = "MzI0NTJlOGQtNzI0YS00ZGQxLWI2ZWYtZGNmYTI2ZWVhZGFj"#Secret
    auth_endpoint = "https://api.8x8.com/oauth/v2/token"
    config_df = session.sql("SELECT * FROM CONFIG_TABLE")

    config_rows = config_df.collect()
    
    try:
        
        for config_row in config_rows:
            Data_Source_Name = config_row['DATA_SOURCE_NAME']
            Table_Name = config_row['TABLE_NAME']
            Table_Schema = config_row['TABLE_SCHEMA']
            Create_Table_SQL = config_row['CREATE_TABLE_SQL']
            Create_Report= config_row['CREATE_REPORT']
            Report_URL = config_row['REPORT_URL']
            Base_URL = config_row['BASE_URL']
            Report_Type = config_row['REPORT_TYPE']
            Relative_URL = config_row['RELATIVE_URL']
            Status_URL = config_row['STATUS_URL']
            Start_Time = config_row['START_TIME']
            End_Time = config_row['END_TIME']
            Pagination_Type = config_row['PAGINATION_TYPE']
            Write_Mode = config_row['WRITE_MODE']
            Load_Type = config_row['LOAD_TYPE']
            Report_Group_By = config_row['REPORT_GROUP_BY']
            Report_Granularity = config_row['REPORT_GRANULARITY']
            Water_Mark_Column   = config_row['WATER_MARK_COLUMN']
            Page_Start_Index = config_row['PAGE_START_INDEX']

            # Get Bearer token
            bearer_token = get_bearer_token(username, password, auth_endpoint)

        
            # URL for the API endpoint
            url = F"{Base_URL}/{Relative_URL}"
            
            report_url = F"{Base_URL}/{Report_URL}"
            

            
        
            create_table_if_not_exists(session,Create_Table_SQL)

            table_schema_array = [col.strip() for col in Table_Schema.split(",")]

            #return str(table_schema_array)
            # Headers
            headers = {
                'Authorization':f"Bearer {bearer_token}",
                'Accept': 'application/json'
            }
            final_result = []

            
            if Load_Type == 'MONTHLY_RANGE_LOAD':
                HARDCODED_START = datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    
                # Handling case where Start_Time and End_Time is null
    
                if Start_Time is None and Load_Type == 'FULL_LOAD':
                    Start_Time = HARDCODED_START  # fixed UTC start for full load
                else:
                    if Start_Time is None:
                        try:
                            # Try to read latest watermark
                            max_start_time_row = session.sql(
                                f'SELECT MAX({Water_Mark_Column}) AS TIME FROM {Table_Name}'
                            ).collect()[0]
                            Start_Time = max_start_time_row['TIME']
                            if Start_Time is None:
                                # MAX returned NULL (no values)
                                raise ValueError("No watermark value")
                            # Advance by 1 ms to avoid reprocessing the max record
                            Start_Time = Start_Time + timedelta(milliseconds=1)
                        except Exception:
                            # Fallback when watermark is missing/NULL or query fails
                            Start_Time = HARDCODED_START
    
                if Start_Time.tzinfo is None:
                    Start_Time = Start_Time.replace(tzinfo=timezone.utc)
    
                
    
                if End_Time is None:
                    End_Time = datetime.now(timezone.utc)  # set to current UTC time
                # ensure end_time is UTC-aware (optional)
                if End_Time.tzinfo is None:
                    End_Time = End_Time.replace(tzinfo=timezone.utc)
                
                
                 
                    
                monthly_ranges = generate_monthly_date_ranges(Start_Time.strftime('%Y-%m-%d'),End_Time.strftime('%Y-%m-%d'))
    
                
                for rng in monthly_ranges:
                    final_result.append(create_report_and_insert_data(
                        username,
                        password,
                        auth_endpoint,                                              
                        Table_Name, 
                        url, 
                        Status_URL,
                        table_schema_array,
                        Write_Mode, 
                        Create_Report,
                        session,
                        report_url,
                        headers,
                        datetime.strptime(rng['start'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc),
                        datetime.strptime(rng['end'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc),
                        Report_Type, 
                        Data_Source_Name,
                        Base_URL,
                        Pagination_Type,
                        Report_Granularity,
                        Report_Group_By,
                        Water_Mark_Column,
                        Page_Start_Index)
                    )
                
            else:
                final_result.append(create_report_and_insert_data(
                        username,
                        password,
                        auth_endpoint,                                              
                        Table_Name, 
                        url, 
                        Status_URL,
                        table_schema_array,
                        Write_Mode, 
                        Create_Report,
                        session,
                        report_url,
                        headers,
                        Start_Time,
                        End_Time,
                        Report_Type, 
                        Data_Source_Name,
                        Base_URL,
                        Pagination_Type,
                        Report_Granularity,
                        Report_Group_By,
                        Water_Mark_Column,
                        Page_Start_Index)
                    )
                


        return str(final_result)
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error occurred: {errh}")
        return f"HTTP Error occurred: {errh}"
    except requests.exceptions.ConnectionError as errc:
        print(f"Error connecting to API: {errc}")
        return f"Error connecting to API: {errc}"
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error occurred: {errt}")
        return f"Timeout Error occurred: {errt}"
    except requests.exceptions.RequestException as err:
        print(f"Something went wrong: {err}")
        return f"Something went wrong: {err}"

$$;


CALL SP_8x8_API_LOAD();

-- select * from CONFIG_TABLE
-- truncate table CONFIG_TABLE






