SELECT 
monitoring_id,
presta_channel_id,
channel_name,
channel_type,
m.creation_ts as monitoring_date,
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
FROM {{ref('stg_monitoring')}} as m
LEFT JOIN  {{ref('int_tracks')}} as t USING (track_id) 
LEFT JOIN {{ref('int_users')}} USING (user_id)
WHERE user_category <> 'rightsnow'
