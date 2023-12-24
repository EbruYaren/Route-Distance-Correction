select order_id, courier_id, date_l, order_domain, step, osrm, km
from franchise_financial_analytics.fleet_vehicle_km_logs
where date_l between '2023-01-30' and '2023-01-31' and osrm is not Null;