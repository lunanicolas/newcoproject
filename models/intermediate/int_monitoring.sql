--modify channel_type with new mapping 
SELECT
monitoring_id
,presta_channel_id
,mon.presta_channel_name
--change channel type
--,channel_type -- old channel type
,map.type as clean_channel_type
,monitoring_date
,played_duration
,track_id
--columns sacem
,map.id_sacem as sacem_channel_id
,map.name_sacem as sacem_channel_name
,map.droits_sacem
FROM {{ref('stg_monitoring')}} as mon
LEFT JOIN {{source('projet_wagon', 'mapping_clean661')}} as map USING (presta_channel_id)