
DELETE FROM etl.operation_analytics.route_dist_correction_route
where _id_oid in
      (select rm._id_oid
       from etl.operation_analytics.routes_market rm
       where checkout_date_l < dateadd(day, -31, '{query_start}')
       {warehouse_filter}
      )
;

select count(*)
from etl.operation_analytics.route_dist_correction_route;