-- FILTER monitoring and remove tracks uploaded by rightsnow (165 tracks - 750k monitoring)
SELECT 
monitoring_id
,presta_channel_id
,channel_name
,channel_type
,m.creation_ts
,played_duration
,track_id
FROM `dbt_lnicolas.stg_monitoring` as m
LEFT JOIN  `dbt_lnicolas.stg_tracks` USING (track_id)
LEFT JOIN `dbt_lnicolas.int_users` USING (user_id)
WHERE user_category <> 'rightsnow'