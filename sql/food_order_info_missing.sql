with km_null_table as (

    select fo._id_oid as order_id,
           nvl(deliverdate, canceldate) as deliver_date,
           convert_timezone('UTC', wl.country_time_zone, deliver_date) as deliver_date_l
    from etl.etl_food_order.foodorders fo
             inner join etl.market_analytics.warehouse_locations wl on fo.courierwarehouse_oid = wl.warehouse
              left join  etl.operation_analytics.route_dist_correction_food rdcf on rdcf._id_oid = fo._id_oid

    where (status in (900, 1000) or (fo.courier_oid IS NOT NULL and canceldate IS NOT NULL))
      and deliver_date >= '{query_start}'
      and deliver_date < '{query_end}'
      and (deliverytype is null or deliverytype = 1)
       and ( rdcf.store_to_rest is null or  rdcf.rest_to_client is null or rdcf.client_to_store is null)
       {warehouse_filter}
),
     order_table as (
         select fo._id_oid                                                as order_id,
                nvl(deliverdate, canceldate)                              as deliver_date,
                convert_timezone('UTC', wl.country_time_zone, deliver_date)   as deliver_date_l,
                date(deliver_date_l)                                      as day,
                NVL(courierwarehouse_oid, courierstore_oid)               as warehouse_id,
                courier_oid                                               as courier_id,
                w.location__coordinates_lon                               as warehouse_lon,
                w.location__coordinates_lat                               as warehouse_lat,
                restaurantloc_lon                                         as restaurant_lon,
                restaurantloc_lat                                         as restaurant_lat,
                deliveryaddress_location__coordinates_lon                 as to_lon,
                deliveryaddress_location__coordinates_lat                 as to_lat
         from etl.etl_food_order.foodorders fo
                  inner join etl.etl_market_warehouse.warehouses w on fo.courierwarehouse_oid = w._id_oid
                  inner join etl.market_analytics.warehouse_locations wl on fo.courierwarehouse_oid = wl.warehouse

         where deliver_date >= '{query_start}'
           and deliver_date < '{query_end}'
           and (fo.status in (900, 1000) or (courier_id IS NOT NULL and canceldate IS NOT NULL))
           and (deliverytype is null or deliverytype = 1)
           {warehouse_filter}
     ),
     avg_table as (select courierwarehouse_oid,
                          avg(store_to_rest)   as store_to_rest_avg,
                          avg(rest_to_client)  as rest_to_client_avg,
                          avg(client_to_store) as client_to_store_avg
                   from etl.operation_analytics.route_dist_correction_food rdc
                            inner join etl.etl_food_order.foodorders fo on rdc._id_oid = fo._id_oid
                            inner join etl.market_analytics.warehouse_locations wl
                                      on fo.courierwarehouse_oid = wl.warehouse

                   where nvl(deliverdate, canceldate) >= '{query_start}'
                     and nvl(deliverdate, canceldate) < '{query_end}'
                     and (fo.status in (900, 1000) or (fo.courier_oid IS NOT NULL and canceldate IS NOT NULL))
                     and (deliverytype is null or deliverytype = 1)
                     {warehouse_filter}
                   group by 1)
select order_table.*,
       avg_table.store_to_rest_avg,
       avg_table.rest_to_client_avg,
       avg_table.client_to_store_avg
from km_null_table
         inner join order_table on km_null_table.order_id = order_table.order_id
         left join avg_table on order_table.warehouse_id = avg_table.courierwarehouse_oid