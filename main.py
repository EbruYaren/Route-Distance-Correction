import pandas as pd

import src
import src.utils
from src.data_access import *
from src.functions import example_func_sleep
from src.utils import parse_args

start_date = ''
end_date = ''


def main():
    global start_date, end_date
    args = parse_args()
    run_mode = args.run_mode
    iugo_period = args.iugo_period
    domain_type = args.domain_type
    warehouse = args.warehouse.split(',')
    iugo_distances = []

    if args.start_date == -1 or args.end_date == -1:
        if run_mode in ('osrm', 'iugo'):
            end_date = pd.to_datetime('2023-12-08 17:00:00')
            start_date = end_date - datetime.timedelta(hours=3)
        else:
            start_date = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0) - datetime.timedelta(hours=24)
            end_date = start_date + datetime.timedelta(hours=24)
            iugo_period = 'daily'
    else:
        start_date = datetime.datetime.strptime(args.start_date, "%d-%m-%Y,%H:%M")
        end_date = datetime.datetime.strptime(args.end_date, "%d-%m-%Y,%H:%M")

    print('Start Date: ', start_date)
    print('End Date: ', end_date)
    print('Run Mode: ', run_mode)

    all_market_order_distances = []
    market_order_distances = []
    kuzeyden_order_distances = []
    refill_distances = []
    food_order_distances = []
    artisan_order_distances = []
    gps_distances = []
    all_routes = []
    print('Hour: ', datetime.datetime.utcnow().hour)
    # Send previous day's OSRM distances to IUGO
    utc_now = datetime.datetime.utcnow()
    if utc_now.hour == 4 and run_mode == 'osrm':
        start_time = utc_now.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(hours=24)
        end_time = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
        print("Sending OSRM and calculated distance data for IUGO started. ")
        upload_osrm_distances_s3(start_time, end_time)
        print('OSRM and calculated distance data sent to IUGO!')

    # Getting OSRM Distances
    if run_mode in ('osrm', 'gps', 'monthly_missing', 'both'):
        # Getting Order Info
        if run_mode in ('osrm', 'both'):
            if domain_type in ['all', "1", "3"]:
                market_order_distances = get_market_order_info(start_date, end_date, warehouse)
            if domain_type in ['all', "1", "4"]:
                kuzeyden_order_distances = get_kuzeyden_order_info(start_date, end_date, warehouse)
                refill_distances = get_refill_info(start_date, end_date, warehouse)
            if domain_type in ['all', "2"]:
                food_order_distances = get_food_order_info(start_date, end_date, warehouse)
            if domain_type in ['all', "6"]:
                artisan_order_distances = get_artisan_order_info(start_date, end_date, warehouse)

            print('Time After Orders Data Access : ', (datetime.datetime.utcnow()))

        # Getting monthly missing orders
        if run_mode == "monthly_missing":
            market_order_distances = get_market_order_info_missing(start_date, end_date, warehouse)
            print('Time After monthly missing market orders : ', (datetime.datetime.utcnow()))

            kuzeyden_order_distances = get_kuzeyden_order_info_missing(start_date, end_date, warehouse)
            print('Time After monthly missing kuzeyden orders : ', (datetime.datetime.utcnow()))

            food_order_distances = get_food_order_info_missing(start_date, end_date, warehouse)
            print('Time After monthly missing food order Data Access : ', (datetime.datetime.utcnow()))

            artisan_order_distances = get_artisan_order_info_missing(start_date, end_date, warehouse)
            print('Time After monthly missing artisan orders : ', (datetime.datetime.utcnow()))

            refill_distances = []

        print('Length of market orders', len(market_order_distances))
        print('Length of kuzeyden orders ', len(kuzeyden_order_distances))
        print('Length of food orders ', len(food_order_distances))
        print('Length of artisan orders ', len(artisan_order_distances))

        if (run_mode in ('osrm', 'monthly_missing', 'both')) and (len(market_order_distances) +
                                                                  len(kuzeyden_order_distances) +
                                                                  len(refill_distances) +
                                                                  len(food_order_distances) +
                                                                  len(artisan_order_distances)) > 0:
            # Getting OSRM Distances
            if len(market_order_distances):
                market_order_distances = get_osrm_distances(4, market_order_distances, 1, run_mode, refill=False)

            if len(kuzeyden_order_distances):
                kuzeyden_order_distances = get_osrm_distances(3, kuzeyden_order_distances, 4, run_mode, refill=False)

            if len(refill_distances):
                refill_distances = get_osrm_distances(3, refill_distances, 4, run_mode, refill=True)

            if len(food_order_distances):
                food_order_distances = get_osrm_distances(3, food_order_distances, 2, run_mode, refill=False)

            if len(artisan_order_distances):
                artisan_order_distances = get_osrm_distances(3, artisan_order_distances, 6, run_mode, refill=False)

        # Getting GPS Distances from GPS Table
        if run_mode in ('gps', 'both'):
            gps_distances = get_gps_distance_info(start_date, end_date, warehouse)

            # Constructing Routes
            print('Construct Routes Started At: ', (datetime.datetime.utcnow()))
            routes_market_raw = get_routes_market(start_date, end_date, warehouse)
            if len(routes_market_raw):
                all_routes = src.functions.process_routes(routes_market_raw)
                all_routes["_id_oid"] = all_routes["order"]
                all_routes = all_routes[["_id_oid", "time", "lon", "lat"]]

            if len(all_routes):
                upsert_to_redshift_route(all_routes, end_date)

            if TEST is False:
                df_route_left = get_routes_delete(start_date, warehouse)
                print('Old routes were deleted and length of remaining routes table :', len(df_route_left))

            print('Time After Construct Routes : ', (datetime.datetime.utcnow()))

    # Final Column Arrangements

    # Combine Market Order and Kuzeyden Order for all run modes
    if len(market_order_distances) > 0 and len(kuzeyden_order_distances) > 0:
        all_market_order_distances = pd.concat([market_order_distances, kuzeyden_order_distances],
                                               axis=0).reset_index(drop=True)
    elif len(market_order_distances) > 0 and len(kuzeyden_order_distances) == 0:
        all_market_order_distances = market_order_distances.copy()
    elif len(kuzeyden_order_distances) > 0 and len(market_order_distances) == 0:
        all_market_order_distances = kuzeyden_order_distances.copy()

    if len(all_market_order_distances):
        all_market_order_distances.drop_duplicates(keep='first', inplace=True)

    if run_mode in ('osrm', 'monthly_missing', 'both'):
        if len(market_order_distances) or len(kuzeyden_order_distances):
            all_market_order_distances = all_market_order_distances.rename(columns={'order_id': '_id_oid'})
            all_market_order_distances = all_market_order_distances[
                ["_id_oid", "step", "to_lon", "to_lat", "from_lon", "from_lat", "distance", "duration", "distance_type",
                 "updated_at"]]
            all_market_order_distances["distance"] = all_market_order_distances["distance"] / 1000
            all_market_order_distances = all_market_order_distances[pd.notnull(all_market_order_distances['distance'])]

        if len(food_order_distances):
            food_order_distances = set_columns_for_food_and_artisan(food_order_distances, 2)
            food_order_distances.drop_duplicates(keep='first', inplace=True)

        if len(artisan_order_distances):
            artisan_order_distances = set_columns_for_food_and_artisan(artisan_order_distances, 6)
            artisan_order_distances.drop_duplicates(keep='first', inplace=True)

        if len(refill_distances):
            refill_distances = refill_distances.rename(
                columns={'distance': 'refill_distance', 'refill_lon': 'lon', 'refill_lat': 'lat'})
            refill_distances = refill_distances[
                ["warehouse_id", "createdatl", "courier_id", "courier_franchise_id", "lon", "lat", "refill_distance",
                 "duration", "distance_type", "updated_at", "log_id", "type"]]
            refill_distances["refill_distance"] = refill_distances["refill_distance"] / 1000
            refill_distances = refill_distances[pd.notnull(refill_distances['refill_distance'])]
            refill_distances.drop_duplicates(keep='first', inplace=True)

        if len(gps_distances):
            gps_distances = gps_distances.assign(duration=None)
            columns = ["_id_oid", "step", "to_lon", "to_lat", "from_lon", "from_lat", "distance", 'duration',
                       "distance_type", "updated_at"]
            gps_distances = gps_distances[columns]
            gps_distances.drop_duplicates(keep='first', inplace=True)

    print('Time After Final Column Arrangements : ', (datetime.datetime.utcnow()))

    # Test Structure, Print data to excel
    if TEST is True:
        file_name = test_structure(market_order_distances, kuzeyden_order_distances, refill_distances,
                                   food_order_distances, artisan_order_distances, all_market_order_distances,
                                   gps_distances, all_routes, run_mode, start_date)
    # Upsert Data to Redshift
    else:

        if run_mode in ['gps', 'iugo'] or len(all_market_order_distances):
            columns = ['_id_oid', 'step', 'to_lon', 'to_lat', 'from_lon', 'from_lat', 'distance', 'distance_type',
                       'updated_at', 'duration']
            if len(iugo_distances):
                iugo_market_distances = iugo_distances[iugo_distances.domain_type.isin([1, 3, 4])]
                iugo_market_distances = arrange_iugo_columns_for_upsert(1, iugo_market_distances)
                # upsert_to_redshift_dists(iugo_market_distances[columns], end_date)
            if len(all_market_order_distances):
                all_market_order_distances = arrange_osrm_columns_for_upsert(1, all_market_order_distances)
                # upsert_to_redshift_dists(all_market_order_distances[columns], end_date)

        if len(refill_distances):
            columns = ['warehouse_id', 'createdatl', 'courier_id', 'courier_franchise_id', 'lon', 'lat',
                       'refill_distance',
                       'distance_type', 'updated_at', 'duration', 'log_id', 'type']
            if run_mode in ('all', 'osrm'):
                refill_distances = refill_distances[columns]
                upsert_to_redshift_dists_kuzeyden_refill(refill_distances, end_date)

        columns = ['_id_oid', 'store_lon', 'store_lat', 'restaurant_lon', 'restaurant_lat', 'client_lon',
                   'client_lat', 'store_to_rest', 'rest_to_client', 'client_to_store', 'updated_at', 'distance_type',
                   'distance', 'step', 'duration', 'store_to_rest_duration', 'rest_to_client_duration',
                   'client_to_store_duration']
        if len(food_order_distances) or run_mode in ['gps', 'iugo']:
            if len(iugo_distances):
                iugo_food_distances = iugo_distances[iugo_distances.domain_type == 2]
                iugo_food_distances = arrange_iugo_columns_for_upsert(2, pd.DataFrame(iugo_food_distances))
                if iugo_food_distances is not None:
                    upsert_to_redshift_dists_food(iugo_food_distances[columns], end_date)

            if len(food_order_distances):
                food_order_distances = arrange_osrm_columns_for_upsert(2, food_order_distances)
                upsert_to_redshift_dists_food(food_order_distances[columns], end_date)

        if len(artisan_order_distances) or run_mode in ['gps', 'iugo']:
            if len(iugo_distances):
                iugo_artisan_distances = iugo_distances[iugo_distances.domain_type == 6]
                iugo_artisan_distances = arrange_iugo_columns_for_upsert(6, pd.DataFrame(iugo_artisan_distances))
                if iugo_artisan_distances is not None:
                    upsert_to_redshift_dists_artisan(iugo_artisan_distances[columns], end_date)

            if len(artisan_order_distances):
                artisan_order_distances = arrange_osrm_columns_for_upsert(6, artisan_order_distances)
                upsert_to_redshift_dists_artisan(artisan_order_distances[columns], end_date)

        if (run_mode in ('gps', 'both')) and len(gps_distances):
            columns = ["_id_oid", "step", "to_lon", "to_lat", "from_lon", "from_lat", "distance",
                       "distance_type", "updated_at", "duration"]
            gps_distances["updated_at"] = datetime.datetime.utcnow()
            gps_distances["duration"] = None
            gps_distances = gps_distances[columns]
            upsert_to_redshift_dists(gps_distances, end_date)

        print('Time After Upsert Redshift : ', (datetime.datetime.utcnow()))


def get_iugo_distances(start, end, run_mode, iugo_period):
    df = iugoDistances(start, end, run_mode, iugo_period)
    if df is not None:
        df['distance_type'] = 'iugo'
        df['updated_at'] = datetime.datetime.utcnow()
        if len(df):
            df['distance'] = df['distance'] / 1000.0
        if run_mode == 'iugo':
            df["distance"] = df.distance.apply(correct_iugo_distance)
        print('Length of iugo orders:', len(df))

        return df
    else:
        return []


def correct_iugo_distance(x):
    if x > 1000.0:
        r = x / 100000.0
        return round(r, 4)

    return x


def get_osrm_distances(trial_count: int, df: pd.DataFrame, domain_type: int, run_mode: str, refill: bool):
    df["distance_type"] = 'osrm'
    df["updated_at"] = datetime.datetime.utcnow()
    lon1, lat1, lon2, lat2 = None, None, None, None

    if refill:
        lon1, lat1, lon2, lat2 = "refill_lon", "refill_lat", "warehouse_lon", "warehouse_lat"
        df.columns = ['log_id', 'marketorder_oid',
                      'createdat', 'createdatl',
                      'date_l', 'warehouse_id',
                      'name', 'refill_lon',
                      'refill_lat', 'warehouse_lon',
                      'warehouse_lat', 'courier_id',
                      'courier_franchise_id', 'type', 'distance_type',
                      'updated_at']

    elif domain_type == 1:
        df.columns = ['order_id', 'deliver_date',
                      'cancel_date', 'deliver_date_l',
                      'is_batched', 'delivery_batch_index',
                      'max_index', 'delivery_job_oid',
                      'warehouse_id', 'courier_id',
                      'order_number', 'warehouse_lon',
                      'warehouse_lat', 'prev_lon',
                      'prev_lat', 'from_lon',
                      'from_lat', 'to_lon',
                      'to_lat', 'will_return_warehouse',
                      'distance_type', 'updated_at']
        df["step"] = df['delivery_batch_index']
        lon1, lat1, lon2, lat2 = "from_lon", "from_lat", "to_lon", "to_lat"

    elif domain_type == 4:
        lon1, lat1, lon2, lat2 = "from_lon", "from_lat", "to_lon", "to_lat"
        df.columns = ['order_id', 'deliver_date_l',
                      'is_batched', 'delivery_batch_index',
                      'delivery_job_oid', 'warehouse_id',
                      'courier_id', 'courier_fleetvehicle_oid',
                      'order_number', 'warehouse_lon',
                      'warehouse_lat', 'to_lon',
                      'to_lat', 'prev_lon',
                      'prev_lat', 'is_from_warehouse',
                      'from_lon', 'from_lat',
                      'distance_type', 'updated_at']
        if refill is False:
            df["step"] = df['delivery_batch_index']

    elif domain_type in [2, 6]:
        if domain_type == 6:
            df.columns = ['order_id', 'deliver_date', 'deliver_date_l',
                          'day', 'warehouse_id', 'courier_id',
                          'warehouse_lon', 'warehouse_lat', 'restaurant_lon',
                          'restaurant_lat', 'to_lon', 'to_lat',
                          'batch_index', 'distance_type', 'updated_at']
            df["step"] = df['batch_index'].astype('Int64')
        else:
            df.columns = ['order_id', 'deliver_date', 'deliver_date_l',
                          'day', 'warehouse_id', 'courier_id',
                          'warehouse_lon', 'warehouse_lat', 'restaurant_lon',
                          'restaurant_lat', 'to_lon', 'to_lat',
                          'distance_type', 'updated_at']

    for i in range(0, trial_count):
        print('beginning {} info pass {} for domain {} {}'.format(domain_type, i, domain_type,
                                                                  'refill' if refill else ''))
        if domain_type not in [2, 6]:
            if i > 1:
                src.utils.osrmErrorCount = 0
                src.utils.queryCount = 0
                df = osrmQuery(df, OSRM_BATCH_SIZE, lon1, lat1, lon2, lat2, 'distance', 'duration')
                if i == 3:
                    if run_mode == 'monthly_missing':
                        df["distance_type"] = df.apply(
                            lambda x: 'predicted' if x["distance"] is None else x["distance_type"], axis=1)
                        df["distance"] = df.apply(
                            lambda x: x['avg_distance'] if x["distance_type"] == 'predicted' else x["distance"], axis=1)

                    # Batchsiz olan siparişlerin osrm kayıtlarını step -1 olarak kopyalıyoruz
                    if domain_type == 1:
                        lon1, lat1, lon2, lat2 = "to_lon", "to_lat", "warehouse_lon", "warehouse_lat"
                        df_unbatched = df[df['is_batched'] == False]
                        if len(df_unbatched):
                            df_unbatched = set_batched_and_unbatched_distances(df_unbatched, lon1, lat1,
                                                                               lon2,
                                                                               lat2)
                            df = pd.concat([df, df_unbatched], axis=0).reset_index()
                        # Batchli olan siparişlerin dönüşü için adresten depoya osrm sorgusu atıp step -1 olarak yapıyoruz.
                        df_batched = df[(df['will_return_warehouse'] == 1) & (df['is_batched'])]
                        if len(df_batched):
                            df_batched = set_batched_and_unbatched_distances(df_batched, lon1, lat1, lon2, lat2)
                            df = pd.concat([df, df_batched], axis=0).reset_index()
            else:
                df = osrmQuery(df, OSRM_BATCH_SIZE, lon1, lat1, lon2, lat2, 'distance', 'duration')

        # Artisan and Food
        else:
            df = osrmQuery(df, OSRM_BATCH_SIZE, "warehouse_lon", "warehouse_lat", "restaurant_lon", "restaurant_lat",
                            'store_to_rest', 'store_to_rest_duration')
            df = osrmQuery(df, OSRM_BATCH_SIZE, "restaurant_lon", "restaurant_lat", "to_lon", "to_lat",
                            'rest_to_client', 'rest_to_client_duration')
            df = osrmQuery(df, OSRM_BATCH_SIZE, "to_lon", "to_lat", "warehouse_lon", "warehouse_lat",
                            'client_to_store', 'client_to_store_duration')

            if run_mode == 'monthly_missing':
                df["distance_type"] = df.apply(
                    lambda x: 'predicted' if x["store_to_rest"] is None else x["distance_type"], axis=1)
                df["store_to_rest"] = df.apply(
                    lambda x: x['store_to_rest_avg'] if x["distance_type"] == 'predicted' else x["store_to_rest"],
                    axis=1)

                df["distance_type"] = df.apply(
                    lambda x: 'predicted' if x["rest_to_client"] is None else x["distance_type"], axis=1)
                df["store_to_rest"] = df.apply(
                    lambda x: x['rest_to_client_avg'] if x["distance_type"] == 'predicted' else x["rest_to_client"],
                    axis=1)

                df["distance_type"] = df.apply(
                    lambda x: 'predicted' if x["client_to_store"] is None else x["distance_type"], axis=1)
                df["store_to_rest"] = df.apply(
                    lambda x: x['client_to_store_avg'] if x["distance_type"] == 'predicted' else x[
                        "client_to_store_avg"], axis=1)

    return df


def set_batched_and_unbatched_distances(df: pd.DataFrame, lon1, lat1, lon2, lat2):
    df = df.assign(step=-1)
    src.utils.osrmErrorCount = 0
    src.utils.queryCount = 0
    df = osrmQuery(df, OSRM_BATCH_SIZE, lon1, lat1, lon2, lat2, 'distance', 'duration')

    for i in range(0, 2):
        src.utils.osrmErrorCount = 0
        src.utils.queryCount = 0
        df = osrmQuery(df, OSRM_BATCH_SIZE, lon1, lat1, lon2, lat2, 'distance', 'duration')
    df_copy = df.copy()
    df['to_lon'] = df_copy['warehouse_lon']
    df['to_lat'] = df_copy['warehouse_lat']
    df['from_lon'] = df_copy['to_lon']
    df['from_lat'] = df_copy['to_lat']

    return df


def get_gps_distances(start, end, warehouse):
    df = get_gps_distance_info(start, end, warehouse)
    df = df[pd.notnull(df['distance'])]
    df["updated_at"] = datetime.datetime.utcnow()

    # Copy unbatched orders as step = -1
    df_without_batch = df[df['is_batched'] == False]
    if len(df_without_batch):
        df_without_batch = df_without_batch.assign(step=-1)
        df = pd.concat([df, df_without_batch], axis=0).reset_index(drop=True)

    print('Time After Getting GPS Distances: ', (datetime.datetime.utcnow()))

    return df


def set_columns_for_food_and_artisan(df: pd.DataFrame, domain_type: int):
    df = df.rename(
        columns={'order_id': '_id_oid', 'warehouse_lon': 'store_lon', 'warehouse_lat': 'store_lat',
                 'to_lon': 'client_lon', 'to_lat': 'client_lat'})
    columns = ["_id_oid", "store_lon", "store_lat", "restaurant_lon", "restaurant_lat", "client_lon", "client_lat",
               "store_to_rest", "rest_to_client", "client_to_store", "store_to_rest_duration",
               "rest_to_client_duration",
               "client_to_store_duration", "updated_at"]
    if domain_type == 6:
        columns.append("step")

    df = df[columns]
    df["store_to_rest"] = df["store_to_rest"] / 1000
    df["rest_to_client"] = df["rest_to_client"] / 1000
    df["client_to_store"] = df["client_to_store"] / 1000

    df = df[pd.notnull(df['store_to_rest'])]
    df = df[pd.notnull(df['rest_to_client'])]
    df = df[pd.notnull(df['client_to_store'])]

    return df


def test_structure(market_orders: pd.DataFrame, kuzeyden_orders: pd.DataFrame, refill_orders: pd.DataFrame,
                   food_orders: pd.DataFrame, artisan_orders: pd.DataFrame, all_market_orders: pd.DataFrame,
                   gps_distances: pd.DataFrame, all_routes: pd.DataFrame, run_mode: str, start):
    file_name = 'route_dist_correction_data_check' + datetime.datetime.strftime(start, '%Y%m%d') + '_' + '.xlsx'

    if (len(market_orders) or len(kuzeyden_orders)) or len(refill_orders) or len(food_orders) or len(
            gps_distances) or len(all_routes) or len(artisan_orders):

        print('control amaçlı file save and check')
        print(file_name)
        writer = src.utils.getExcelWriter(file_name, src.config.TEST)

        # Write file
        if run_mode in ('osrm', 'monthly_missing', 'both') and (len(market_orders) + len(kuzeyden_orders) +
                                                                len(refill_orders) + len(food_orders) +
                                                                len(artisan_orders)) > 0:

            if len(market_orders) or len(kuzeyden_orders):
                src.utils.convert_mongo_to_str(all_market_orders).to_excel(writer, 'all_market_order_info',
                                                                           index=False)
                worksheet = writer.sheets['all_market_order_info']

            if len(refill_orders):
                src.utils.convert_mongo_to_str(refill_orders).to_excel(writer, 'refill_info', index=False)
                worksheet = writer.sheets['refill_info']
            if len(food_orders):
                src.utils.convert_mongo_to_str(food_orders).to_excel(writer, 'food_order_info', index=False)
                worksheet = writer.sheets['food_order_info']

            if len(artisan_orders):
                src.utils.convert_mongo_to_str(artisan_orders).to_excel(writer, 'artisan_order_info',
                                                                        index=False)
                worksheet = writer.sheets['artisan_order_info']

        if (run_mode in ('gps', 'both')) and len(gps_distances):
            src.utils.convert_mongo_to_str(gps_distances).to_excel(writer, 'gps_distance_info', index=False)
            src.utils.convert_mongo_to_str(all_routes).to_excel(writer, 'allroutes', index=False)
            worksheet = writer.sheets['gps_distance_info']
            worksheet = writer.sheets['allroutes']
            # worksheet.set_column('A:AN', 28)

        writer.save()
    else:
        print('There is no data for this case')

    print('file write ok.')
    return file_name


def arrange_iugo_columns_for_upsert(domain_type: int, df: pd.DataFrame):
    if len(df):
        if domain_type in (2, 6):
            df.rename(columns={'order_id': '_id_oid'}, inplace=True)

            df['store_to_rest'] = None
            df['rest_to_client'] = None
            df['client_to_store'] = None
            df['store_to_rest_duration'] = None
            df['rest_to_client_duration'] = None
            df['client_to_store_duration'] = None
            df['store_lon'] = None
            df['store_lat'] = None
            df['client_lon'] = None
            df['client_lat'] = None
            df['restaurant_lon'] = None
            df['restaurant_lat'] = None
            df['duration'] = None

        elif domain_type in (1, 3):
            df.rename(columns={'order_id': '_id_oid'}, inplace=True)
            df['to_lon'], df['to_lat'], df['from_lon'], df['from_lat'], df['duration'] = None, None, None, None, None

        return df.drop_duplicates()


def arrange_osrm_columns_for_upsert(domain_type: int, df: pd.DataFrame):
    if len(df):
        if 'order_id' in df.columns:
            df.rename(columns={'order_id': '_id_oid'}, inplace=True)
        if domain_type in (2, 6):
            df['distance'] = df['store_to_rest'] + df['rest_to_client'] + df['client_to_store']
            df['duration'] = df['store_to_rest_duration'] + df['rest_to_client_duration'] + df[
                'client_to_store_duration']
            df['distance_type'] = 'osrm'

            if 'order_id' in df.columns:
                df.rename(columns={'order_id': '_id_oid'}, inplace=True)
            df.rename(columns={'warehouse_lon': 'store_lon', 'warehouse_lat': 'store_lat', 'to_lon': 'client_lon',
                               'to_lat': 'client_lat'}, inplace=True)

            if domain_type == 2:
                df['step'] = None

        return df.drop_duplicates()


def upload_osrm_distances_s3(query_start_date, query_end_date):
    df = osrm_and_franchise_earning_distances(query_start_date, query_end_date)
    df.columns = ['order_id', 'courier_id', 'date_l', 'domain_type', 'step', 'osrm', 'distance']
    df_water = get_water_osrm_distances(query_start_date, query_end_date)
    df_water.columns = ['order_id', 'courier_id', 'date_l', 'domain_type', 'step', 'osrm', 'distance']
    all_domains_df = pd.concat([df, df_water], axis=0).reset_index(drop=True)
    date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    file_name = "osrm_and_calculated_distances_%s.csv" % date
    url = upload_to_s3(all_domains_df, file_name)
    json_data = {
        "start_time": query_start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_time": query_end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_link": url
    }
    headers = {
        'Content-Type': 'application/json',
        'x-iugo-api-key': '45010f6c-f802-11ec-b939-0242ac120002'
    }
    requests.post('https://panel.iugo.tech/services/api/v1/getir/osrm_distance', headers=headers, json=json_data)


if __name__ == "__main__":
    main()
