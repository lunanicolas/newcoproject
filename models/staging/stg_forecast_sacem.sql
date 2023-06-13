with histo AS (
    SELECT 
    date_date
    ,id_sacem
    ,name_sacem
    ,ROUND(SUM(avg_value_sec),8) AS tot_val_sec
    FROM {{ref('stg_data_sacem')}} 
    GROUP BY date_date
    ,id_sacem
    ,name_sacem
    ),
mean AS (
    SELECT
    id_sacem,
    name_sacem,
    AVG(tot_val_sec) as avg_tot
    FROM histo
    GROUP BY id_sacem,name_sacem
    )


SELECT 
DATE(2023,7,1) as date_date,
id_sacem,
mean.name_sacem,
CASE 
  WHEN forecast IS NULL THEN ROUND(mean.avg_tot,8)
  ELSE ROUND(forecast,8) 
END AS forecast,
CASE 
  WHEN forecast_lower IS NULL THEN ROUND(mean.avg_tot,8)
  ELSE ROUND(forecast_lower,8) 
END AS forecast_lower
FROM {{source("projet_wagon","predictions_final")}}
FULL OUTER JOIN mean using(id_sacem)