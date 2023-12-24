import logging
from datetime import datetime
from dateutil import tz
from datetime import timedelta
import argparse
from dateutil import relativedelta
import pandas as pd
import re
import boto3
from botocore.exceptions import NoCredentialsError
from src import config
import os
from time import process_time, sleep
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable
from sqlalchemy import VARCHAR, FLOAT, INTEGER, BOOLEAN, TIMESTAMP
import numpy as np
from sqlalchemy import MetaData, Table, Column
import requests


def get_local_current_time(tz_name='Europe/Istanbul'):
    time_zone = tz.gettz(tz_name)
    return datetime.now(time_zone).replace(tzinfo=None)


def read_query(file_path: str, skip_line_count=0):
    """

    :param file_path: file path of your query to read.
    :return: string object of your query
    """
    try:
        with open(file_path, 'r') as file:
            con_str = file.readlines()
    except:
        raise Exception('{} is not found'.format(file_path))
    return ' '.join(con_str[skip_line_count:])


def execute_query(inputs, query_path, engine):
    with engine.begin() as connection:
        query = read_query(query_path).format(**inputs)
        resultproxy = connection.execute(query)

        if resultproxy.returns_rows:
            df = pd.DataFrame([{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy])
            return df


osrmErrorCount = 0
queryCount = 0


def cache(func):
    def wrapper(self, *args, **kwargs):
        __hash_name__ = func.__name__ + str(kwargs) + str(args)
        if __hash_name__ not in self.func_dict.keys():
            self.func_dict[__hash_name__] = func(self, *args, **kwargs)
        return self.func_dict[__hash_name__]

    return wrapper


def timer(print_args=True):
    def decorator(function):
        def wrapper(*args, **kwargs):
            start = get_local_current_time()
            process_start = process_time()
            if print_args:
                print('Function {}, started at {} with parameters args: {}, kwargs: {}'
                      .format(function.__name__, start, args, kwargs))
            else:
                print('Function {}, started at {}'
                      .format(function.__name__, start))
            result = function(*args, **kwargs)
            process_end = process_time()
            end = get_local_current_time()
            total_time = (end - start).seconds
            cpu_time = round(process_end - process_start, 2)
            io_time = total_time - cpu_time
            print('Function {}, ended at {}. TOTAL TIME: {}, CPU TIME: {}, IO TIME: {} seconds.'
                  .format(function.__name__, end, total_time, cpu_time, io_time))
            return result

        return wrapper

    return decorator


def get_local_time(utc_time, timezone='Europe/Istanbul'):
    local_tz = tz.gettz(timezone)
    local_offset = local_tz.utcoffset(utc_time)
    diff_seconds = local_offset.total_seconds()
    local_time = utc_time + timedelta(seconds=diff_seconds)
    return local_time


@timer
def get_run_dates(interval=timedelta(hours=24)):
    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", "--start_date",
                        help="Start Date of the time interval of the cron")
    parser.add_argument("-ed", "--end_date",
                        help="End Date of the time interval of the cron")
    args, leftovers = parser.parse_known_args()
    total_seconds = interval.total_seconds()
    now = get_local_current_time()
    a_day_in_seconds = 60 * 60 * 24
    an_hour_in_seconds = 60 * 60
    a_minute_in_seconds = 60
    dates = []
    start = None

    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        start = start_date
        now = end_date
    else:
        logging.warning('Start and End date is not given. Run dates will be generated for last time interval.'
                        .format((args.start_date, args.end_date)))

    assert a_day_in_seconds >= total_seconds, 'Maximum Time Interval is in days'

    if total_seconds % a_day_in_seconds == 0:

        end = now.replace(hour=0, minute=0, second=0, microsecond=0)

    elif total_seconds % an_hour_in_seconds == 0:
        hours = int(total_seconds / an_hour_in_seconds)
        assert 24 % hours == 0, 'a day is indivisible to {} hours'.format(hours)
        replace_hours = int((now.hour // hours) * hours)
        end = now.replace(hour=replace_hours, minute=0, second=0, microsecond=0)

    elif total_seconds % a_minute_in_seconds == 0:
        minutes = int(total_seconds / a_minute_in_seconds)
        assert 60 % minutes == 0, 'an hour is indivisible to {} minutes'.format(minutes)
        replace_minute = int((now.minute // minutes) * minutes)
        end = now.replace(minute=replace_minute, second=0, microsecond=0)

    else:
        assert False, 'Minimum Time Interval is in minutes'

    if start:
        pass
    else:
        start = end - interval

    while end >= start:
        dates.append(start)
        start += interval
    logging.info("Start date: {}, end date: {}, time interval: {} ".format(dates[0], dates[-1], interval))
    return dates


def upload_to_s3(local_file, bucket, s3_file, s3_access_key, s3_secret_key):
    s3 = boto3.client('s3', aws_access_key_id=s3_access_key,
                      aws_secret_access_key=s3_secret_key)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def upsert_redshift(df, target, unique_id, engine, s3_bucket_name, s3_region,
                    s3_access_key, s3_secret_key):
    """

    :param df:
    :param target:
    :param unique_id:
    :param engine:
    :param s3_access_key:
    :param s3_secret_key:
    :return:
    """

    s3_client = boto3.client('s3', aws_access_key_id=s3_access_key,
                             aws_secret_access_key=s3_secret_key,
                             region_name=s3_region)

    # write csv file to cache folder
    file_name = 'client-dates-%s.csv' % datetime.now().strftime("%Y%m%d")
    df.to_csv(file_name, index=False)
    print('DONE: write csv file to cache folder')
    # upload csv file to s3
    s3_client.upload_file(file_name, s3_bucket_name, file_name)
    print('DONE: upload csv file to s3')
    query_temp = read_query("./sql/upsert_redshift.sql")

    query = query_temp.format(target=target,
                              file_name=file_name,
                              s3_bucket_name=s3_bucket_name,
                              s3_access_key=s3_access_key,
                              s3_secret_key=s3_secret_key,
                              s3_region=s3_region,
                              unique_id=unique_id)

    with engine.begin() as connection:
        connection.execute(query, config.redshift_db)

    if os.path.exists(file_name):
        os.remove(file_name)
    s3_client.delete_object(Bucket=config.S3_BUCKET_NAME, Key=file_name)


def _pd_type_to_sql_alchemy(given_by_user):
    type_to_sql_alch = {
        np.dtype('O'): VARCHAR(128),
        np.dtype('float64'): FLOAT,
        np.dtype('int64'): INTEGER,
        np.dtype('M'): TIMESTAMP,
        np.dtype('b'): BOOLEAN,
        np.dtype('<M8[ns]'): TIMESTAMP,
    }
    if given_by_user:
        return {**type_to_sql_alch, **given_by_user}
    return type_to_sql_alch


def _generate_columns(name_to_type, type_mapping):
    columns = set(Column(column_name, type_mapping[column_type])
                  for column_name, column_type in name_to_type.items())
    return columns


def create_table_from_df(table_name, df, schema=None, dialect=postgresql.dialect()):
    pd_type_to_alch = _pd_type_to_sql_alchemy(schema)
    name_to_type = df.dtypes.to_dict()
    metadata = MetaData()
    result_table = Table(table_name, metadata, *_generate_columns(name_to_type, pd_type_to_alch))
    final_statement = CreateTable(result_table)
    return final_statement.compile(dialect=dialect).__str__()


def snake_case_fixer(df):
    """
    This function takes input data frame and returns the same data frame with renamed columns for snake case words.
    Ex: "item_name" => "Item Name"
    :param df:
    :return: df with renamed columns
    """
    new_columns = {}
    for column in df.columns:
        renamed_column = ' '.join(word.title() for word in column.split('_'))
        new_columns[column] = renamed_column
    df = df.rename(columns=new_columns)
    return df





def parse_args():
    """
    parses the command line args and returns
    the parsed args.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--domain', default='both')
    parser.add_argument('--start_date', default=-1)
    parser.add_argument('--end_date', default=-1)
    parser.add_argument("--run_mode", help="Run mode", default='osrm')
    # warehouse exp: '5ede92c8e500970451d54278' OR '5ede92c8e500970451d54278,6038d821393fcd9bc1996403'
    parser.add_argument("--warehouse", default='all')

    parser.add_argument("--iugo_period", default='hourly')

    parser.add_argument("--domain_type", default='4')

    parsed = parser.parse_args()

    assert parsed.domain in ['getir', 'market', 'both', 'food', 'kuzeyden',
                             'kuzeyden_refill'], "domain should be 'kuzeyden', 'getir', 'market', 'food' or 'both'."
    assert parsed.run_mode in ['gps', 'all', 'osrm', 'both', 'food', 'kuzeyden', 'kuzeyden_refill', 'monthly_missing', 'iugo'], "run mode should be 'kuzeyden', 'gps', 'osrm', 'iugo', 'food' or 'both'."

    assert parsed.iugo_period in ['hourly', 'daily'], "iugo period should be daily or hourly"

    assert parsed.domain_type in ['all', "1", "2", "3", "4", "6"], "Domain type should be in [all, 1, 2, 3, 4, 6]"

    return parsed


def decode_time(encoded):
    """
    splits encoded time offset values to list of float.
    Each element in the list is the time passed from start
    time, in seconds.
    """
    if len(encoded):
        return [int(i, 36) / 20 if len(i) else None for i in encoded.split(";")]
    else:
        return None


def getLocalTime(utcTime):
    local_tz = tz.gettz('Europe/Istanbul')
    local_offset = local_tz.utcoffset(utcTime)
    diff_seconds = local_offset.total_seconds()
    localTime = utcTime + timedelta(seconds=diff_seconds)
    return localTime


def getExcelWriter(fileName, isTest, datetimeFormatted=False):
    print('isTest', isTest)
    if isTest:
        file_dir = './%s' % fileName
    else:
        file_dir = 'output/%s' % fileName
    if datetimeFormatted:
        writer = pd.ExcelWriter(file_dir, date_format="mmm d yyyy hh AM/PM",
                                datetime_format="mmm d yyyy hh AM/PM")
    else:
        writer = pd.ExcelWriter(file_dir)
    print('file directory is %s' % file_dir)

    return writer


def convert_mongo_to_str(df):
    def is_mongo_id(x):
        s = str(x)
        prog = re.compile("^[a-f\d]{24}$")
        result = bool(prog.match(s))
        if result:
            return s
        else:
            return x

    df2 = df.applymap(lambda x: is_mongo_id(x))
    return df2



git remote add origin https://github.com/EbruYaren/Route-Distance-Correction.git
git branch -M main
git push -u origin main