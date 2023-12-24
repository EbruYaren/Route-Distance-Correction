select csl._id_oid                         as log_id,
       csl.marketorder_oid,
       csl.createdat,
       createdatl,
       date(createdatl)                    as date_l,
       warehouse_oid                       as warehouse_id,
       w.name,
       csl.location__coordinates_lon       as refill_lon,
       csl.location__coordinates_lat       as refill_lat,
       w.location__coordinates_lon         as warehouse_lon,
       w.location__coordinates_lat         as warehouse_lat,
       courier_oid                         as courier_id,
       currentmarketemployer_franchise_oid as courier_franchise_id,
       case when data_reason_oid = '641428b2eb07104f026b6eb5' then 'busy_return_warehouse' else 'refill' end as type
from etl_getir_logs.courierstatuslogs csl
         left join etl.etl_market_warehouse.warehouses w on csl.warehouse_oid = w._id_oid
         left join etl.market_analytics.warehouse_locations wl on w._id_oid = wl.warehouse

         left join (select _p_id_oid
                    from etl_market_warehouse.warehouses__domaintypes
                    where domaintypes = 4
                    group by 1) wd on csl.warehouse_oid = wd._p_id_oid

where (((data_reason_oid = '5ece95a7f0cc3239856b2511' AND newstatus = 200) or newstatus = 4100)
        or data_reason_oid = '641428b2eb07104f026b6eb5' AND newstatus = 200 and oldstatus = 900)
  and csl.createdat >= '{query_start}'
  and csl.createdat < '{query_end}'
  and date_part(hour, createdatl) not in (2, 3, 4, 5, 6, 7)
  {warehouse_filter}
  ;