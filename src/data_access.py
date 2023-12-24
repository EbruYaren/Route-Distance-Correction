from src.utils import *
from src.config import *
from src.constants import *
from botocore.client import Config
import time
import pandas as pd
from src.utils import cache, timer, read_query
from src.config import REDSHIFT_ETL_LIVE
from src.config import S3_REGION, S3_BUCKET_NAME, REDSHIFT_ETL_IAM_ROLE
import boto3
import datetime
import requests
import json

class DataAccess:
    def __init__(self):
        self.func_dict = {}

    @cache
    def get_data(self, a, b):
        print("Inputs are", a, b)
        time.sleep(3)
        return "Courier dict is ready!"

    @timer
    def get_another_data(self, x):
        time.sleep(x)


def get_market_order_info(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND mo.warehouse_warehouse_oid IN {}'.format(in_clause)
    query = read_query('./sql/market_order_info.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df


def get_market_order_info_missing(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND mo.warehouse_warehouse_oid IN {}'.format(in_clause)
    query = read_query('./sql/market_order_info_missing.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df


def get_food_order_info(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND fo.courierwarehouse_oid IN {}'.format(in_clause)
    query = read_query('./sql/food_order_info.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df


def get_food_order_info_missing(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND fo.courierwarehouse_oid IN {}'.format(in_clause)
    query = read_query('./sql/food_order_info_missing.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df


def get_artisan_order_info(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND fo.courierwarehouse_oid IN {}'.format(in_clause)
    query = read_query('./sql/artisan_order_info.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df


def get_artisan_order_info_missing(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND fo.courierwarehouse_oid IN {}'.format(in_clause)
    query = read_query('./sql/artisan_order_info_missing.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df


def get_kuzeyden_order_info(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
        warehouse_warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND mo.warehouse_warehouse_oid IN {}'.format(in_clause)
        warehouse_warehouse_filter = 'AND csl.warehouse_oid IN {}'.format(in_clause)
    query = read_query('./sql/kuzeyden_order_info.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter,
                         warehouse_warehouse_filter=warehouse_warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)

    return df


def get_kuzeyden_order_info_missing(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
        warehouse_warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND mo.warehouse_warehouse_oid IN {}'.format(in_clause)
        warehouse_warehouse_filter = 'AND csl.warehouse_oid IN {}'.format(in_clause)
    query = read_query('./sql/kuzeyden_order_info_missing.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter,
                         warehouse_warehouse_filter=warehouse_warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df


def get_refill_info(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND csl.warehouse_oid IN {}'.format(in_clause)
    query = read_query('./sql/refill_info.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df


def get_gps_distance_info(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND mo.warehouse_warehouse_oid IN {}'.format(in_clause)
    query = read_query('./sql/gps_distance_info.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df


def get_routes_market(query_start, query_end, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND rm.warehouse IN {}'.format(in_clause)
    query = read_query('./sql/gps_route_market.sql')
    query = query.format(query_start=query_start, query_end=query_end, warehouse_filter=warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df


def osrm_and_franchise_earning_distances(query_start, query_end):
    with FRANCHISE_CLUSTER.cursor() as connection:
        query = read_query('./sql/osrm_and_franchise_earning_km.sql')
        # query = query.format(query_start=query_start, query_end=query_end)
        cursor = connection.execute(query)
        df = pd.DataFrame(cursor.fetchall())
    """query = read_query('./sql/osrm_and_franchise_earning_km.sql')
    query = query.format(query_start=query_start, query_end=query_end)
    df = pd.read_sql_query(query, REDSHIFT_FRANCHISE_CLUSTER)"""
    return df



def get_water_osrm_distances(query_start, query_end):
    query = read_query('./sql/getir_water_distances.sql')
    #query = query.format(query_start=query_start, query_end=query_end)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df



def get_routes_delete(query_start, warehouse):
    if 'all' in warehouse:
        warehouse_filter = ''
    else:
        in_clause = _create_in_clause(warehouse)
        warehouse_filter = 'AND rm.warehouse IN {}'.format(in_clause)
    query = read_query('./sql/route_delete.sql')
    query = query.format(query_start=query_start, warehouse_filter=warehouse_filter)
    df = pd.read_sql_query(query, REDSHIFT_ETL_LIVE)
    return df



def osrmQuery(df: pd.DataFrame, batch_size: int, lon1: str, lat1: str, lon2: str, lat2: str, distance_column: str, duration_column: str):
    url = 'http://map-engine-gateway.algo.data.getirapi.com'
    c = -1
    batch_size = batch_size // 2
    for i in range(0, len(df), batch_size):
        c += 1
        lst = []
        for current_index in range(i, min(i + batch_size, len(df))):
            row = df.iloc[current_index]
            lst.append([row[lon1], row[lat1]])
            lst.append([row[lon2], row[lat2]])
        url = 'http://map-engine-gateway.algo.data.getirapi.com'
        response = requests.post(
            f'{url}/services/table/car',
            headers={'GETIR_MAPS_API_KEY': config.GETIR_MAPS_API_KEY},
            json={"coordinates": lst, "annotations": ["distance", "duration"],
                  "sources": list(range(0, len(lst), 2)),
                  "destinations": list(range(1, len(lst), 2))}).json()

        distances = response.get('distances')
        durations = response.get('durations')
        distances_list = np.diag(distances)
        duration_list = np.diag(durations)


        df.loc[df.index[c*50:current_index+1], distance_column] = distances_list
        df.loc[df.index[c * 50:current_index + 1], duration_column] = duration_list



    return df




def iugoDistances(start_date, end_date, run_mode, iugo_period):
    IUGOErrorCount = 0
    dataCount = 0
    dataCount += 1
    if queryCount % 1000 == 0:
        print(queryCount)
    if IUGOErrorCount > 6000:
        return None

    if run_mode not in ['iugo', 'gps']:
        end_date = end_date.replace(minute=0, second=0) - timedelta(hours=6)
        start_date = end_date - timedelta(minutes=90)

    print('Iugo start time: ', start_date)
    print('Iugo end time: ', end_date)
    if iugo_period == 'daily':
        start = 'start_time'
        end = 'end_time'
    else:
        start = 'processed_start_time'
        end = 'processed_end_time'

    json_data = {
        start: start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end: end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    URL = 'https://panel.iugo.tech/services/api/v1/getir/distance'
    headers = {'x-iugo-api-key': IUGO_API_KEY}
    print('Headers: ', headers)
    print('Type of Headers: ', type(headers))
    response = requests.post(URL, headers=headers, json=json_data)
    if response.status_code == 200:
        data = response.json()
        result = pd.DataFrame(data.get('data'))
        return result


def upsert_to_redshift_dists_food(resultDf, end):
    """
    Flow:
    - Dataframe is written to csv.
    - Csv file uploaded to S3.
    - Table is created if it did not exist
    - Csv file is copied(inserted) to temp table from S3.
    - Data is inserted to actual table from temp table.
    """
    # Filename
    file_name = 'file%s' % end.strftime("%Y%m%d")
    # Writing to csv
    resultDf.to_csv(file_name, index=False)
    # Uploading csv file to s3
    s3_client = boto3.client('s3', region_name=S3_REGION)
    # upload csv file to s3
    s3_client.upload_file(file_name, S3_BUCKET_NAME, file_name)
    print('DONE: upload csv file to s3')
    query = """
    CREATE TABLE IF NOT EXISTS {target}(
    --Field names of the new table should be here, example:
    _id_oid VARCHAR(26) SORTKEY,
    store_lon FLOAT,
    store_lat FLOAT,
    restaurant_lon FLOAT,
    restaurant_lat FLOAT,
    client_lon FLOAT,
    client_lat FLOAT,
    store_to_rest FLOAT,
    rest_to_client FLOAT,
    client_to_store FLOAT,
    updated_at TIMESTAMP,
    distance_type VARCHAR(100),
    distance FLOAT,
    step INT,
    duration FLOAT,
    store_to_rest_duration FLOAT,
    rest_to_client_duration FLOAT,
    client_to_store_duration FLOAT
    );

    create temp table IF NOT EXISTS {temptarget} (like {target});

    copy {temptarget} from 's3://{S3_BUCKET_NAME}/{fileName}'
    iam_role '{REDSHIFT_IAM_ROLE}'
    region '{S3_REGION}' delimiter ',' ignoreheader 1;
    begin transaction;
    delete from {target}
    using {temptarget}
    where {target}._id_oid = {temptarget}._id_oid and {target}.distance_type = {temptarget}.distance_type;
    insert into {target}
    select * from {temptarget};
    end transaction;
    """.format(target='operation_analytics.route_dist_correction_food',
               temptarget='temp_route_dist_correction_food',
               fileName=file_name,
               S3_BUCKET_NAME=S3_BUCKET_NAME,
               REDSHIFT_IAM_ROLE=REDSHIFT_ETL_IAM_ROLE,
               S3_REGION=S3_REGION)
    result_proxy = pd.io.sql.execute(query, REDSHIFT_ETL_LIVE)
    result_proxy.close()


def upsert_to_redshift_dists_artisan(resultDf, end):
    """
    Flow:
    - Dataframe is written to csv.
    - Csv file uploaded to S3.
    - Table is created if it did not exist
    - Csv file is copied(inserted) to temp table from S3.
    - Data is inserted to actual table from temp table.
    """
    # Filename
    file_name = 'file%s' % end.strftime("%Y%m%d")
    # Writing to csv
    resultDf.to_csv(file_name, index=False)
    # Uploading csv file to s3
    s3_client = boto3.client('s3', region_name=S3_REGION)
    # upload csv file to s3
    s3_client.upload_file(file_name, S3_BUCKET_NAME, file_name)
    print('DONE: upload csv file to s3')
    query = """
        CREATE TABLE IF NOT EXISTS {target}(
        --Field names of the new table should be here, example:
        _id_oid VARCHAR(26) SORTKEY,
        store_lon FLOAT,
        store_lat FLOAT,
        restaurant_lon FLOAT,
        restaurant_lat FLOAT,
        client_lon FLOAT,
        client_lat FLOAT,
        store_to_rest FLOAT,
        rest_to_client FLOAT,
        client_to_store FLOAT,
        updated_at TIMESTAMP,
        distance_type VARCHAR(100),
        distance FLOAT,
        step INT,
        duration FLOAT,
        store_to_rest_duration FLOAT,
        rest_to_client_duration FLOAT,
        client_to_store_duration FLOAT
        );

        create temp table IF NOT EXISTS {temptarget} (like {target});

        copy {temptarget} from 's3://{S3_BUCKET_NAME}/{fileName}'
        iam_role '{REDSHIFT_IAM_ROLE}'
        region '{S3_REGION}' delimiter ',' ignoreheader 1;
        begin transaction;
        delete from {target}
        using {temptarget}
        where {target}._id_oid = {temptarget}._id_oid and {target}.distance_type = {temptarget}.distance_type;
        insert into {target}
        select * from {temptarget};
        end transaction;
        """.format(target='operation_analytics.route_dist_correction_artisan',
                   temptarget='temp_route_dist_correction_artisan',
                   fileName=file_name,
                   S3_BUCKET_NAME=S3_BUCKET_NAME,
                   REDSHIFT_IAM_ROLE=REDSHIFT_ETL_IAM_ROLE,
                   S3_REGION=S3_REGION)
    result_proxy = pd.io.sql.execute(query, REDSHIFT_ETL_LIVE)
    result_proxy.close()


def upsert_to_redshift_dists_kuzeyden_refill(resultDf, end):
    """
    Flow:
    - Dataframe is written to csv.
    - Csv file uploaded to S3.
    - Table is created if it did not exist
    - Csv file is copied(inserted) to temp table from S3.
    - Data is inserted to actual table from temp table.
    """
    # Filename
    file_name = 'file%s' % end.strftime("%Y%m%d")
    # Writing to csv
    resultDf.to_csv(file_name, index=False)
    # Uploading csv file to s3
    s3_client = boto3.client('s3', region_name=S3_REGION)
    # upload csv file to s3
    s3_client.upload_file(file_name, S3_BUCKET_NAME, file_name)
    print('DONE: upload csv file to s3')

    query = """
    CREATE TABLE IF NOT EXISTS {target}(
    --Field names of the new table should be here, example:
    warehouse_id VARCHAR(26),
    createdatl TIMESTAMP SORTKEY,
    courier_id VARCHAR(26),
    courier_franchise_id VARCHAR(26),
    lon FLOAT,
    lat FLOAT,
    refill_distance FLOAT,
    distance_type VARCHAR(64),
    updated_at TIMESTAMP,
    duration FLOAT,
    log_id VARCHAR(26),
    type VARCHAR(26)
    );

    create temp table IF NOT EXISTS {temptarget} (like {target});
    -- alter table {target} add column type VARCHAR(26) DEFAULT 'refill' ;
    copy {temptarget} from 's3://{S3_BUCKET_NAME}/{fileName}'
    iam_role '{REDSHIFT_IAM_ROLE}'
    region '{S3_REGION}' delimiter ',' ignoreheader 1;
    begin transaction;
    delete from {target}
    using {temptarget}
    where {target}.courier_id = {temptarget}.courier_id and {target}.createdatl = {temptarget}.createdatl;
    insert into {target}
    select * from {temptarget};
    end transaction;
    """.format(target='operation_analytics.route_dist_correction_kuzeyden_refill',
               temptarget='temp_route_dist_correction_kuzeyden_refill',
               fileName=file_name,
               S3_BUCKET_NAME=S3_BUCKET_NAME,
               REDSHIFT_IAM_ROLE=REDSHIFT_ETL_IAM_ROLE,
               S3_REGION=S3_REGION)
    result_proxy = pd.io.sql.execute(query, REDSHIFT_ETL_LIVE)
    result_proxy.close()


def upsert_to_redshift_dists(resultDf, end):
    """
    Flow:
    - Dataframe is written to csv.
    - Csv file uploaded to S3.
    - Table is created if it did not exist
    - Csv file is copied(inserted) to temp table from S3.
    - Data is inserted to actual table from temp table.
    """
    # Filename
    file_name = 'file%s' % end.strftime("%Y%m%d")
    # Writing to csv
    resultDf.to_csv(file_name, index=False)
    # Uploading csv file to s3
    s3_client = boto3.client('s3', region_name=S3_REGION)
    # upload csv file to s3
    s3_client.upload_file(file_name, S3_BUCKET_NAME, file_name)
    print('DONE: upload csv file to s3')
    query = """
    CREATE TABLE IF NOT EXISTS {target}(
    --Field names of the new table should be here, example:
    _id_oid VARCHAR(26) SORTKEY,
    step INTEGER,
    to_lon FLOAT,
    to_lat FLOAT,
    from_lon FLOAT,
    from_lat FLOAT,
    distance FLOAT,
    distance_type VARCHAR(64),
    updated_at TIMESTAMP,
    duration FLOAT
    );

    create temp table IF NOT EXISTS {temptarget} (like {target});
    -- alter table {target} add column duration FLOAT DEFAULT Null;
    copy {temptarget} from 's3://{S3_BUCKET_NAME}/{fileName}' 
    iam_role '{REDSHIFT_IAM_ROLE}'
    region '{S3_REGION}' delimiter ',' ignoreheader 1;
    begin transaction;
    delete from {target}
    using {temptarget}
    where {target}._id_oid = {temptarget}._id_oid and {target}.distance_type = {temptarget}.distance_type;
    insert into {target}
    select * from {temptarget};
    end transaction;
    """.format(target='operation_analytics.route_dist_correction',
               temptarget='temp_route_dist_correction',
               fileName=file_name,
               S3_BUCKET_NAME=S3_BUCKET_NAME,
               REDSHIFT_IAM_ROLE=REDSHIFT_ETL_IAM_ROLE,
               S3_REGION=S3_REGION)
    result_proxy = pd.io.sql.execute(query, REDSHIFT_ETL_LIVE)
    result_proxy.close()


def upsert_to_redshift_route(resultDf, end):
    """
    Flow:
    - Dataframe is written to csv.
    - Csv file uploaded to S3.
    - Table is created if it did not exist
    - Csv file is copied(inserted) to temp table from S3.
    - Data is inserted to actual table from temp table.
    """
    # Filename
    file_name = 'file%s' % end.strftime("%Y%m%d")
    # Writing to csv
    resultDf.to_csv(file_name, index=False)
    # Uploading csv file to s3
    s3_client = boto3.client('s3', region_name=S3_REGION)
    # upload csv file to s3
    s3_client.upload_file(file_name, S3_BUCKET_NAME, file_name)
    print('DONE: upload csv file to s3')
    query = """
    CREATE TABLE IF NOT EXISTS {target}(
    --Field names of the new table should be here, example:
    _id_oid VARCHAR(26) SORTKEY,
    time TIMESTAMP,
    lon FLOAT,
    lat FLOAT
    );

    create temp table IF NOT EXISTS {temptarget} (like {target});
    -- alter table {target} add duration FLOAT;
    copy {temptarget} from 's3://{S3_BUCKET_NAME}/{fileName}'
    iam_role '{REDSHIFT_IAM_ROLE}'
    region '{S3_REGION}' delimiter ',' ignoreheader 1;
    begin transaction;
    delete from {target}
    using {temptarget}
    where {target}._id_oid = {temptarget}._id_oid;
    insert into {target}
    select * from {temptarget};
    end transaction;
    """.format(target='operation_analytics.route_dist_correction_route',
               temptarget='temp_route_dist_correction_route',
               fileName=file_name,
               S3_BUCKET_NAME=S3_BUCKET_NAME,
               REDSHIFT_IAM_ROLE=REDSHIFT_ETL_IAM_ROLE,
               S3_REGION=S3_REGION)
    result_proxy = pd.io.sql.execute(query, REDSHIFT_ETL_LIVE)
    result_proxy.close()


def get_load_error():
    query = """
    select le.starttime, d.query, d.line_number, d.colname, d.value,
le.raw_line, le.err_reason    
from stl_loaderror_detail d, stl_load_errors le
where d.query = le.query
order by le.starttime desc
limit 100
    """
    return pd.read_sql(query, REDSHIFT_ETL_LIVE)


def _create_in_clause(id_s: list):
    if len(id_s) == 0:
        return ''
    ids = ["'" + str(id_) + "'" for id_ in id_s]

    return "(" + ','.join(ids) + ")"



def upload_to_s3(df, file_name):
    """
    uploads the processed dataframe to s3.

    :param file_name: local and remote file name
    :param df: processed dataframe.
    :return: returns the s3 file name, str.
    """
    print('Df length: ', len(df))
    print('Df columns: ', df.columns)
    df.to_csv(file_name, index=False, header=True)
    s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))
    s3_client.upload_file(file_name, config.S3_BUCKET_NAME, file_name)
    url = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': config.S3_BUCKET_NAME,
            'Key': file_name
        },
        ExpiresIn=172800  # 2 days
    )

    return url

