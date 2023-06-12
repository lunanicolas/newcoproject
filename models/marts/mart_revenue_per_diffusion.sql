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
,track_category
--columns from int_mapping_value
,name_sacem
,type
,tot_val_sec
-- valued seconds
,ROUND((played_duration*tot_val_sec),2) AS revenue_per_diffusion
FROM {{ref("mart_master_monitoring")}} mon
JOIN {{ref("int_mapping_value")}} map USING (presta_channel_id)
WHERE date_date LIKE '%072022'
ORDER BY monitoring_date