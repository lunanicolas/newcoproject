with mean AS (
  SELECT
  id_sacem,
  AVG(avg_value_sec) as avg_tot
  FROM {{ref('mart_sacem_value')}}
  GROUP BY id_sacem
)

SELECT 
clean.id_sacem,
clean.channel_name_presta,
CASE 
  WHEN forecast IS NULL THEN ROUND(mean.avg_tot,8)
  ELSE ROUND(forecast,8) END AS forecast,
FROM {{source("projet_wagon","mapping_clean")}}`rightsnow-385413.projet_wagon.mapping_clean` clean 
LEFT JOIN {{source("projet_wagon","predictions_final")}} predict using(id_sacem)
JOIN mean using(id_sacem)