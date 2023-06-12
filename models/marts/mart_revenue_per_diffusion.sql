SELECT 
monitoring_id,
sacem_channel_id,
sacem_channel_name,
presta_channel_id,
presta_channel_name,
clean_channel_type,
monitoring_date,
played_duration,
track_id,
artist,
title,
duration,
track_upload_date,
original_filename,
user_id,
email,
first_name,
last_name,
broadcast_count,
sub_date,
track_count,
user_category,
track_category,
--columns from stg_data_sacem
name_sacem,
tot_val_sec,
-- valued seconds
ROUND((played_duration*tot_val_sec),2) AS revenue_per_diffusion
FROM {{ref("mart_master_monitoring")}} mon
JOIN {{ref("int_data_sacem_concat")}} map USING (presta_channel_id)
WHERE date_date = '2022-07-01'
ORDER BY monitoring_date