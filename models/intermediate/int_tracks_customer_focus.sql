-- FILTER track catalog and remove tracks uploaded by rightsnow (165 tracks)
SELECT 
track_id
,artist
,title
,duration
,user_id
,creation_ts
,original_filename
FROM `dbt_lnicolas.stg_tracks`
LEFT JOIN `dbt_lnicolas.int_users` USING (user_id)
WHERE user_category <> 'rightsnow'