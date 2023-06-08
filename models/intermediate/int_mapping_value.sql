  SELECT 
  date_date,
  sac.name_sacem,
  channel_name_presta,
  channel_type,
  ROUND(SUM(CASE WHEN type_droit LIKE 'DROIT EXECUTION PUBLIQUE' THEN val_sec ELSE 0 END),8) AS droit_execution_publique,
  ROUND(SUM(CASE WHEN type_droit LIKE 'DROIT DE REPRODUCTION' THEN val_sec ELSE 0 END),8) AS droit_reproduction,
  ROUND(SUM(val_sec),8) as tot_val_sec
  FROM {{ source('projet_wagon', 'mapping_clean') }} clean_map
  LEFT JOIN {{ref("stg_data_sacem")}} sac USING (id_sacem)
  LEFT JOIN {{ source('projet_wagon', 'mapping_sacem') }} map ON clean_map.id_sacem = SAFE_CAST(map.id_sacem AS INT64)
  GROUP BY 1,2,3,4
  ORDER BY 1 DESC