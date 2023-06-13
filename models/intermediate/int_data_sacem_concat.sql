
-- Concatenate historical values and prediction 
-- select histo date 
SELECT 
date_date
,id_sacem
,name_sacem
,ROUND(SUM(avg_value_sec),8) AS tot_val_sec
,NULL as forecast_lower
FROM {{ref('stg_data_sacem')}} 
GROUP BY date_date
,id_sacem
,name_sacem


UNION ALL 

-- select pred data
SELECT 
date_date
,id_sacem
,name_sacem
,forecast AS tot_val_sec
,forecast_lower
FROM {{ref('stg_forecast_sacem')}} 


