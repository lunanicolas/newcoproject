SELECT
monitoring_id
,presta_channel_id
,channel_name
,channel_type
,monitoring_date
,played_duration
,track_id
,artist
,title
,duration
,track_upload_date
,original_filename
,user_id
,email
,first_name
,last_name
,broadcast_count
,sub_date
,track_count
,user_category
,CASE
    WHEN REGEXP_CONTAINS(original_filename, 'official|video|clip|orelsan|booba|gta') OR broadcast_count > 3000 THEN 'famous_track'
    ELSE 'proper_track' 
END AS track_category
FROM `rightsnow-385413.dbt_rightsnow_marts.mart_master_monitoring`