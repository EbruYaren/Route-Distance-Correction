with order_table as (
    select fo._id_oid                                                 as order_id,
           nvl(deliverdate, canceldate)                               as deliver_date,
           nvl(convert_timezone('UTC', wl.country_time_zone, deliverdate), convert_timezone('UTC', wl.country_time_zone, canceldate))  as deliver_date_l,
           date(deliver_date_l)                                       as day,
           NVL(courierwarehouse_oid, courierstore_oid)                as warehouse_id,
           courier_oid                                                as courier_id,
           w.location__coordinates_lon                                as warehouse_lon,
           w.location__coordinates_lat                                as warehouse_lat,
           restaurantloc_lon                                          as restaurant_lon,
           restaurantloc_lat                                          as restaurant_lat,
           deliveryaddress_location__coordinates_lon                  as to_lon,
           deliveryaddress_location__coordinates_lat                  as to_lat,
           ct.batch_index
    from etl.etl_artisan_order.foodorders fo
             left join etl.etl_market_warehouse.warehouses w on NVL(courierwarehouse_oid, courierstore_oid) = w._id_oid
             left join etl.market_analytics.warehouse_locations wl on NVL(courierwarehouse_oid, courierstore_oid) = wl.warehouse
             left join (select orderid_oid, sequence, ROW_NUMBER () OVER (partition by taskgroupid_oid order by sequence) as batch_index
             			from etl_artisan_courier.couriertasks
             			where type = 'Delivery' and status in ('COMPLETED', 'CANCELLED')) ct ON fo._id_oid = ct.orderid_oid
    where deliver_date >= '{query_start}'
      and deliver_date < '{query_end}'
      and (fo.status in (900, 1000) or (courier_id IS NOT NULL and canceldate IS NOT NULL))
      and deliverytype = 1
      {warehouse_filter}
)
select *
from order_table;