with mean AS (
  SELECT
  id_sacem,
  name_sacem,
  AVG(avg_value_sec) as avg_tot
  FROM {{ref('mart_sacem_value')}}
  GROUP BY id_sacem,name_sacem
)

SELECT 
clean.id_sacem,
mean.name_sacem,
clean.channel_name_presta,
CASE 
  WHEN forecast IS NULL THEN ROUND(mean.avg_tot,8)
  ELSE ROUND(forecast,8) END AS forecast,
CASE 
  WHEN forecast_lower IS NULL THEN ROUND(mean.avg_tot,8)
  ELSE ROUND(forecast_lower,8) END AS forecast_lower
FROM {{source("projet_wagon","mapping_clean661")}} clean 
LEFT JOIN {{source("projet_wagon","predictions_final")}} predict using(id_sacem)
JOIN mean using(id_sacem)