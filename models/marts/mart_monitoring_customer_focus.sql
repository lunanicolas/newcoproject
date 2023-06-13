-- FILTER monitoring and remove tracks uploaded by rightsnow (165 tracks - 750k monitoring)
SELECT 
monitoring_id
,sacem_channel_id
,sacem_channel_name
,presta_channel_id
,presta_channel_name
,clean_channel_type
,monitoring_date
,played_duration
,track_id
FROM {{ref('mart_master_monitoring')}} as m
WHERE user_category <> 'rightsnow'
