  SELECT 
  date_date,
  clean_map.name_sacem,
  clean_map.presta_channel_id,
  channel_name_presta,
  type,
  ROUND(SUM(CASE WHEN type_droit LIKE 'DROIT EXECUTION PUBLIQUE' THEN val_sec ELSE 0 END),8) AS droit_execution_publique,
  ROUND(SUM(CASE WHEN type_droit LIKE 'DROIT DE REPRODUCTION' THEN val_sec ELSE 0 END),8) AS droit_reproduction,
  ROUND(SUM(val_sec),8) as tot_val_sec
  FROM {{ source('projet_wagon', 'mapping_clean') }} clean_map
  LEFT JOIN {{ref("stg_data_sacem")}} sac USING (id_sacem)
  GROUP BY 1,2,3,4,5
  ORDER BY 1 DESC