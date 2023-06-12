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
--SAFE_DIVIDE(broadcast_count, track_count) AS avg_broadcast
FROM {{ref('int_monitoring')}} as m
LEFT JOIN  {{ref('int_tracks')}} as t USING (track_id) 
LEFT JOIN {{ref('int_users')}} USING (user_id)
