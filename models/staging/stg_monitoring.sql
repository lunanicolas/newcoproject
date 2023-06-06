SELECT 
id as monitoring_id
--,active
--,archived
,channel_id as presta_channel_id
,channel_name
,channel_type
,creation_ts
--,internal_comment
--,iso_date_utc
--,last_modification_ts
,played_duration
,track as track_id
FROM `rightsnow-385413.projet_wagon.monitoring`