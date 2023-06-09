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
t.creation_ts as track_upload_date,
original_filename,
t.user_id,
email,
first_name,
last_name,
broadcast_count,
sub_date,
track_count,
user_category,
CASE
    WHEN user_id = 156 OR user_id = 460 OR user_id = 366 OR user_id = 286 THEN 'test_track'
    WHEN REGEXP_CONTAINS(original_filename, 'official|video|clip|orelsan|booba|gta') OR broadcast_count > 4000 THEN 'famous_track'
    ELSE 'emerging_track' 
END AS track_category,
SAFE_DIVIDE(broadcast_count, track_count) AS avg_broadcast
FROM {{ref('stg_monitoring')}} as m
LEFT JOIN  {{ref('stg_tracks')}} as t USING (track_id) 
LEFT JOIN {{ref('int_users')}} USING (user_id)
WHERE user_category <> 'rightsnow'
