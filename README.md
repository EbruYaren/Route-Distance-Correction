# cron_template

##Dependent Crons:
None
## Cron Information

* Frequency: Daily
* Parameters: 

start_date:yyyy-mm-dd
end_date:yyyy-mm-dd
               
* Description: Slices the batched courier routes and their distances, queries osrm and GPS data to get distance info.

### Databases & Environment Variables

| Database          | Permission        | Collections        | Environment Name            
| ----------------- |:----------------: | ------------------ | ---------------------------
| REDSHIFT_ETL         | r/w | etl_market_order.marketorders, operation_analytics.routes_getir, operation_analytics.routes_market, etl_getir.orders | REDSHIFT_ETL_URI        
| S3_ACCESS_KEY         |     -        | -  | S3_ACCESS_KEY        
| S3_SECRET_KEY           |         -         | -    |          S3_SECRET_KEY
| S3_REGION           |         -         | -    |          S3_REGION
| S3_BUCKET_NAME           |         -        | -    |         S3_BUCKET_NAME          




### Optional Environment Variables
Ex:
* <api_token_name>
* <slack_hook_url>
* <run_environment>





