SELECT 
creation_ts,
channel_name_presta,
name_sacem,
type,
played_duration,
tot_val_sec,
ROUND((played_duration*tot_val_sec),2) AS revenue_per_diffusion
FROM {{ref("stg_monitoring")}} mon
JOIN {{ref("int_mapping_value")}} map USING (presta_channel_id)
WHERE date_date LIKE '%072022'
ORDER BY creation_ts