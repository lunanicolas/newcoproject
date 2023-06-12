--modify channel_type with new mapping 
SELECT
monitoring_id
,presta_channel_id
,channel_name
--change channel type
--,channel_type -- old channel type
,map.type as clean_channel_type
,creation_ts
,played_duration
,track_id
--columns sacem
,map.id_sacem
,map.name_sacem
,map.droits_sacem
FROM {{ref('stg_monitoring')}} as mon
LEFT JOIN FROM {{source('projet_wagon', 'mapping_clean661')}} as map USING (presta_channel_id)