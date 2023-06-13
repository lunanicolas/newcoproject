-- FILTER track catalog and remove tracks uploaded by rightsnow (165 tracks)
SELECT 
track_id
,artist
,title
,duration
,user_id
,track_upload_date
,original_filename
FROM {{ref('int_tracks')}}
LEFT JOIN {{ref('int_users')}} USING (user_id)
WHERE user_category <> 'rightsnow'