select m._id_oid as order_id, m.courier_courier_oid, date_trunc('day', m.checkoutdatel) as date_l, 4 as domaintype,
       r.step, case when r.distance > 13 then 13.0 else r.distance end as osrm,
       osrm as km
from etl_market_order.marketorders m
join operation_analytics.route_dist_correction r on m."_id_oid"  = r."_id_oid"
where m.checkoutdatel between '2023-01-30' and '2023-01-31' and r.distance_type = 'osrm' and m.domaintype = 4