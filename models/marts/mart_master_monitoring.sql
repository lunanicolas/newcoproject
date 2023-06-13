WITH pred AS (SELECT * FROM {{ref("int_data_sacem_concat")}} WHERE date_date = '2023-07-01')

SELECT 
monitoring_id,
sacem_channel_id,
sacem_channel_name,
presta_channel_id,
presta_channel_name,
clean_channel_type,
monitoring_date,
played_duration,
m.track_id,
artist,
title,
duration,
track_upload_date,
original_filename,
t.user_id,
email,
first_name,
last_name,
broadcast_count,
sub_date,
track_count,
user_category,
track_category,
tot_val_sec,
ROUND((played_duration * tot_val_sec),2) as revenue_per_diffusion,
forecast_lower,
ROUND((played_duration * forecast_lower),2) as lower_revenue_per_diffusion
--SAFE_DIVIDE(broadcast_count, track_count) AS avg_broadcast
FROM {{ref('int_monitoring')}} as m
LEFT JOIN  {{ref('int_tracks')}} as t USING (track_id) 
LEFT JOIN {{ref('int_users')}} USING (user_id)
LEFT JOIN pred ON pred.id_sacem = sacem_channel_id