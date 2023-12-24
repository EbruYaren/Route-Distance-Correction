select rm._id_oid, encoded_latlons, encoded_time, start_time
from operation_analytics.routes_market rm
      left join etl_market_order.marketorders as mo on mo._id_oid = rm._id_oid
         left join etl.market_analytics.warehouse_locations wl on mo.warehouse_warehouse_oid = wl.warehouse
where nvl(deliver_date, cancel_date) >= '{query_start}'
  and nvl(deliver_date, cancel_date) < '{query_end}'
  {warehouse_filter}