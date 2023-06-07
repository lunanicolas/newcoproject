SELECT 
monitoring_id
,presta_channel_id
,channel_name
,channel_type
,m.creation_ts
,played_duration
,m.track_id
,artist
,title
,duration
,t.creation_ts
,original_filename
,t.user_id
,email
,first_name
,last_name
,broadcast_count
,sub_date
,track_count
,user_category
FROM {{ref('stg_monitoring')}} as m
LEFT JOIN  {{ref('stg_tracks')}} as t USING (track_id) 
LEFT JOIN {{ref('int_users')}} USING (user_id)
WHERE user_category <> 'rightsnow'
