select *
from (
         with km_null_table as (
             select mo._id_oid                                                      as order_id,
                    mo.domaintype,
                    nvl(mo.deliver_date, mo.cancel_date)                            as deliver_date,
                    convert_timezone('UTC', wl.country_time_zone, deliver_date)         as deliver_date_l,
                    mo.delivery_batch_isbatched                                     as is_batched,
                    NVL(mo.delivery_batch_index, 1)                                 as delivery_batch_index,
                    mo.delivery_job_oid
             from etl_market_order.marketorders mo
             left join etl.market_analytics.warehouse_locations wl on mo.warehouse_warehouse_oid = wl.warehouse

             left join (select rdc._id_oid, distance
                    from etl.operation_analytics.route_dist_correction rdc
                             inner join etl_market_order.marketorders mo on rdc._id_oid = mo._id_oid
                             left join etl.market_analytics.warehouse_locations wl
                                       on mo.warehouse_warehouse_oid = wl.warehouse

                    where distance_type in ('osrm', 'predicted')
                      and nvl(mo.deliver_date, mo.cancel_date) >= '{query_start}'
                      and nvl(mo.deliver_date, mo.cancel_date) < '{query_end}'
                      and rdc.distance is not null
                      and mo.domaintype in (1, 3)
                      {warehouse_filter}
                   ) x on mo._id_oid=x._id_oid

             where (status in (900, 1000) or (mo.courier_courier_oid IS NOT NULL and cancel_date IS NOT NULL))
               and nvl(mo.deliver_date, mo.cancel_date) >= '{query_start}'
               and nvl(mo.deliver_date, mo.cancel_date) < '{query_end}'
               and mo.domaintype in (1, 3)
               and distance is null
               {warehouse_filter}
         ),
              order_table as (
                  select _id_oid                                                                            as order_id,
                         warehouse_warehouse_oid                                                            as warehouse_id,
                         courier_courier_oid                                                                as courier_id,
                         delivery_job_oid,
                         nvl(convert_timezone('UTC', wl.country_time_zone, deliver_date), convert_timezone('UTC', wl.country_time_zone, cancel_date))                        as deliver_date_l,
                         date_trunc('day', convert_timezone('UTC', wl.country_time_zone, deliver_date_l))   as day,
                         delivery_address_location__coordinates_lon                                         as to_lon,
                         delivery_address_location__coordinates_lat                                         as to_lat,
                         warehouse_location__coordinates_lon                                                as warehouse_lon,
                         warehouse_location__coordinates_lat                                                as warehouse_lat,
                         lag(to_lon, 1)
                         over (partition by warehouse_warehouse_oid,courier_id order by deliver_date_l)     as prev_lon,
                         lag(to_lat, 1)
                         over (partition by warehouse_warehouse_oid,courier_id order by deliver_date_l)     as prev_lat,
                         ROW_NUMBER()
                         over (partition by day,warehouse_warehouse_oid,courier_id order by deliver_date_l) as order_number
                  from etl_market_order.marketorders mo
                           left join etl.market_analytics.warehouse_locations wl
                                     on mo.warehouse_warehouse_oid = wl.warehouse

                  where nvl(mo.deliver_date, mo.cancel_date) >= dateadd(hour, -2, '{query_start}')
                    and nvl(mo.deliver_date, mo.cancel_date) < '{query_end}'
                    and (status in (900, 1000) or (mo.courier_courier_oid IS NOT NULL and cancel_date IS NOT NULL))
                    and mo.domaintype in (1, 3)
                    {warehouse_filter}
                    -- and mo.country_oid in ('61370da7a937eb4e96795fed', '61370da7a937eb4e96795fec', '61370da7a937eb4e96795fee')
              ),
              batch_max_table as (
                  select delivery_job_oid,
                         max(delivery_batch_index) as max_index
                  from etl_market_order.marketorders mo
                           left join etl.market_analytics.warehouse_locations wl
                                     on mo.warehouse_warehouse_oid = wl.warehouse

                  where nvl(mo.deliver_date, mo.cancel_date) >= dateadd(hour, -2, '{query_start}')
                    and nvl(mo.deliver_date, mo.cancel_date) < '{query_end}'
                    and (status in (900, 1000) or (mo.courier_courier_oid IS NOT NULL and cancel_date IS NOT NULL))
                    and delivery_batch_isbatched is True
                    and mo.domaintype in (1, 3)
                    {warehouse_filter}
                    -- and mo.country_oid in ('61370da7a937eb4e96795fed', '61370da7a937eb4e96795fec', '61370da7a937eb4e96795fee')
                  group by 1),

              avg_distance_table as (
                   select mo.warehouse_warehouse_oid as warehouse_id,
                         avg(distance)              as avg_distance
                  from etl.operation_analytics.route_dist_correction rdc
                           left join etl_market_order.marketorders mo on rdc._id_oid = mo._id_oid
                           left join etl.market_analytics.warehouse_locations wl
                                     on mo.warehouse_warehouse_oid = wl.warehouse

                  where nvl(mo.deliver_date, mo.cancel_date) >= '{query_start}'
                    and nvl(mo.deliver_date, mo.cancel_date) < '{query_end}'
                    and (status in (900, 1000) or (mo.courier_courier_oid IS NOT NULL and cancel_date IS NOT NULL))
                    and mo.domaintype in (1, 3)
                    {warehouse_filter}
                    -- and mo.country_oid in ('61370da7a937eb4e96795fed', '61370da7a937eb4e96795fec', '61370da7a937eb4e96795fee')
                  group by 1
              )
         select nl.order_id,
                nl.deliver_date_l,
                nl.deliver_date,
                nl.is_batched,
                nl.delivery_batch_index,
                NVL(b.max_index, 1)                                                     as max_index,
                nl.delivery_job_oid,
                o.warehouse_id,
                o.courier_id,
                o.order_number,
                o.warehouse_lon                                                         as warehouse_lon,
                o.warehouse_lat                                                         as warehouse_lat,
                o.prev_lon,
                o.prev_lat,
                case when (delivery_batch_index = 1 or delivery_batch_index is null)  then warehouse_lon else prev_lon end as from_lon,
                case when (delivery_batch_index = 1 or delivery_batch_index is null)  then warehouse_lat else prev_lat end as from_lat,
                o.to_lon,
                o.to_lat,
                av.avg_distance,
                case when delivery_batch_index = max_index then 1 else 0 end               will_return_warehouse

         from km_null_table as nl
                  inner join order_table o on nl.order_id = o.order_id
                  left join batch_max_table b on o.delivery_job_oid = b.delivery_job_oid
                  left join avg_distance_table av on o.warehouse_id = av.warehouse_id
     ) a

where deliver_date >= '{query_start}'
  and deliver_date < '{query_end}';