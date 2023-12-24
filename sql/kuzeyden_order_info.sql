with order_table as
         (select _id_oid                                                                                                                                            as order_id,
                 warehouse_warehouse_oid                                                                                                                            as warehouse_id,
                 courier_courier_oid                                                                                                                                as courier_id,
                 courier_fleetvehicle_oid,
                 nvl(deliver_date, cancel_date)                                                                                                                     as deliver_date,
                                  nvl(date_trunc('day',
                            convert_timezone('UTC', wl.country_time_zone, deliver_date)), date_trunc('day',
                            convert_timezone('UTC', wl.country_time_zone, cancel_date)))                                                                                  as day,
                 nvl(convert_timezone('UTC', wl.country_time_zone, deliver_date), convert_timezone('UTC', wl.country_time_zone, cancel_date))                  as deliver_date_l,
                 mo.delivery_batch_isbatched                                                                                                                        as is_batched,
                 NVL(mo.delivery_batch_index, 1)                                                                                             as delivery_batch_index,
                 mo.delivery_job_oid,
                 delivery_address_location__coordinates_lon                                                                                                         as to_lon,
                 delivery_address_location__coordinates_lat                                                                                                         as to_lat,
                 warehouse_location__coordinates_lon                                                                                                                as warehouse_lon,
                 warehouse_location__coordinates_lat                                                                                                                as warehouse_lat,
                 lag(to_lon, 1)
                 over (partition by courier_fleetvehicle_oid,warehouse_warehouse_oid order by convert_timezone('UTC', 'Europe/Istanbul', deliver_date) ::timestamp) as prev_lon,
                 lag(to_lat, 1)
                 over (partition by courier_fleetvehicle_oid,warehouse_warehouse_oid order by convert_timezone('UTC', 'Europe/Istanbul', deliver_date) ::timestamp) as prev_lat,
                 ROW_NUMBER()
                 over (partition by day,courier_id order by convert_timezone('UTC', 'Europe/Istanbul', deliver_date) ::timestamp)                                   as order_number
          from etl_market_order.marketorders mo
                  left join etl.market_analytics.warehouse_locations wl on mo.warehouse_warehouse_oid = wl.warehouse

          where nvl(deliver_date, cancel_date) >= dateadd(hour, -24, '{query_start}')
            and nvl(deliver_date, cancel_date) < '{query_end}'
            and (status in (900, 1000) or (courier_id IS NOT NULL and cancel_date IS NOT NULL))
            and domaintype = 4
            {warehouse_filter}
         ),
     after_refill as (
         select next_order as order_id, 1 as is_from_warehouse
         from (
                  select *
                  from (
                           select csl._id_oid                                                                   as id,
                                  csl.createdat,
                                  convert_timezone('UTC', wl.country_time_zone, csl.createdat)  as compare_date_l,
                                  csl.warehouse_oid                                                             as warehouse_id,
                                  csl.courier_oid                                                               as courier_id,
                                  'refill'                                                                      as type,
                                  csl.data_plate                                                                as vehicle,
                                  date(compare_date_l)                                                          as day,
                                  data_reason_oid,
                                  lag(marketorder_oid, 1) IGNORE NULLS
                                      over (partition by warehouse_id, courier_id order by compare_date_l desc) as next_order
                           from etl_getir_logs.courierstatuslogs csl
                                    left join etl.etl_market_warehouse.warehouses w on csl.warehouse_oid = w._id_oid
                                    left join etl.market_analytics.warehouse_locations wl on w._id_oid = wl.warehouse

                           where  csl.createdat >= '{query_start}'
                             and  csl.createdat < dateadd(hour, 2, '{query_end}')
                             {warehouse_warehouse_filter}
                             )
                  where data_reason_oid = '5ece95a7f0cc3239856b2511'
                    and createdat >= '{query_start}'
                    and createdat < '{query_end}'
              ))
select o.order_id,
       -- o.deliver_date,
       o.deliver_date_l,
       o.is_batched,
       o.delivery_batch_index,
       o.delivery_job_oid,
       o.warehouse_id,
       o.courier_id,
       o.courier_fleetvehicle_oid,
       o.order_number,
       o.warehouse_lon              as warehouse_lon,
       o.warehouse_lat              as warehouse_lat,
       o.to_lon,
       o.to_lat,
       o.prev_lon,
       o.prev_lat,
       NVL(ar.is_from_warehouse, 0) as is_from_warehouse,
       case when is_from_warehouse = 1 then warehouse_lon else prev_lon end as from_lon,
       case when is_from_warehouse = 1 then warehouse_lat else prev_lat end as from_lat

from order_table o
         left join after_refill ar
                   on o.order_id = ar.order_id
where o.deliver_date >= '{query_start}'
  and o.deliver_date < '{query_end}'