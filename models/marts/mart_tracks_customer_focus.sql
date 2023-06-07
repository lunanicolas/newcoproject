-- FILTER track catalog and remove tracks uploaded by rightsnow (165 tracks)
SELECT 
t.track_id
,artist
,title
,duration
,user_id
,creation_ts
,original_filename
FROM {{ref('stg_tracks')}} as t
LEFT JOIN {{ref('int_users')}} USING (user_id)
WHERE user_category <> 'rightsnow'