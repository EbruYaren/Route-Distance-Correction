with gps_table as
         (
             select rm._id_oid,
                    step,
                    corrected_distance                                          as filtered_route_length,
                    nvl(deliver_date, cancel_date) as deliver_date,
                    nvl(convert_timezone('UTC', wl.country_time_zone, deliver_date), convert_timezone('UTC', wl.country_time_zone, cancel_date)) as deliver_date_l,
                    'gps'                                                       as type
             from operation_analytics.orders_distances as rm
                      left join etl_market_order.marketorders as mo on mo._id_oid = rm._id_oid
                      left join etl.market_analytics.warehouse_locations wl on mo.warehouse_warehouse_oid = wl.warehouse

             where deliver_date >= '{query_start}'
               and deliver_date < '{query_end}'
               {warehouse_filter}
         ),

     order_info_table as (
         select *
         from (
                  with order_table as (
                      select _id_oid                                                                                as order_id,
                             warehouse_warehouse_oid                                                                as warehouse_id,
                             courier_courier_oid                                                                    as courier_id,
                             delivery_job_oid,
                             mo.delivery_batch_isbatched                                                            as is_batched,
                             NVL(mo.delivery_batch_index, 1)                                                        as delivery_batch_index,
                             nvl(deliver_date, cancel_date)                                                         as deliver_date,
                             convert_timezone('UTC', wl.country_time_zone, deliver_date) as deliver_date_l,
                             date_trunc('day',
                                        convert_timezone('UTC', wl.country_time_zone, deliver_date_l))              as day,
                             delivery_address_location__coordinates_lon                                             as to_lon,
                             delivery_address_location__coordinates_lat                                             as to_lat,
                             warehouse_location__coordinates_lon                                                    as warehouse_lon,
                             warehouse_location__coordinates_lat                                                    as warehouse_lat,
                             lag(to_lon, 1)
                             over (partition by warehouse_warehouse_oid,courier_id order by deliver_date_l)         as prev_lon,
                             lag(to_lat, 1)
                             over (partition by warehouse_warehouse_oid,courier_id order by deliver_date_l)         as prev_lat,
                             ROW_NUMBER()
                             over (partition by day,warehouse_warehouse_oid,courier_id order by deliver_date_l asc) as order_number
                      from etl_market_order.marketorders mo
                               left join etl.market_analytics.warehouse_locations wl
                                         on mo.warehouse_warehouse_oid = wl.warehouse
                      where deliver_date >= dateadd(hour, -2, '{query_start}')
                        and deliver_date < '{query_end}'
                        and (status in (900, 1000) or (courier_id IS NOT NULL and cancel_date IS NOT NULL))
                        and mo.domaintype in (1, 3)
                        {warehouse_filter}
                  ),

                       batch_max_table as (
                           select delivery_job_oid,
                                   max(delivery_batch_index) as max_index
                            from etl_market_order.marketorders mo
                            left join etl.market_analytics.warehouse_locations wl on mo.warehouse_warehouse_oid = wl.warehouse
                            where nvl(deliver_date, cancel_date) >= dateadd(hour, -2, '{query_start}')
                             and nvl(deliver_date, cancel_date) < '{query_end}'
                             and (status in (900, 1000) or (mo.courier_courier_oid IS NOT NULL and cancel_date IS NOT NULL))
                             and delivery_batch_isbatched is True
                             and mo.domaintype in (1, 3)
                             {warehouse_filter}
                            group by 1)

                  select o.order_id,
                         o.deliver_date,
                         o.deliver_date_l,
                         o.is_batched,
                         o.delivery_batch_index,
                         NVL(b.max_index, 1)                                                     as max_index,
                         o.delivery_job_oid,
                         o.warehouse_id,
                         o.courier_id,
                         o.order_number,
                         o.warehouse_lon                                                         as warehouse_lon,
                         o.warehouse_lat                                                         as warehouse_lat,
                         o.prev_lon,
                         o.prev_lat,
                         case when delivery_batch_index = 1 then warehouse_lon else prev_lon end as from_lon,
                         case when delivery_batch_index = 1 then warehouse_lat else prev_lat end as from_lat,
                         o.to_lon,
                         o.to_lat,
                         case when delivery_batch_index = max_index then 1 else 0 end               will_return_warehouse

                  from order_table o
                           left join batch_max_table b on o.delivery_job_oid = b.delivery_job_oid
                  order by o.delivery_job_oid asc) a

         where deliver_date >= '{query_start}'
           and deliver_date < '{query_end}')

select _id_oid,
       NVL(delivery_batch_index, 1)    as step,
       to_lon,
       to_lat,
       from_lon,
       from_lat,
       gps_table.filtered_route_length as distance,
       type                            as distance_type,
       is_batched
from gps_table
         left join order_info_table on gps_table._id_oid = order_info_table.order_id