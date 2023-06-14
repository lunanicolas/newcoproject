-- categorize tracks
SELECT 
track_id
,artist
,title
,duration
,t.user_id
,t.track_upload_date
,original_filename
,CASE
    WHEN user_id = 156 OR user_id = 366 OR user_id = 286 OR user_category = 'rightsnow' THEN 'test_track'
    WHEN REGEXP_CONTAINS(original_filename, 'official|video|clip|orelsan|booba|gta') OR broadcast_count > 4000 THEN 'famous_track'
    ELSE 'emerging_track' 
END AS track_category
FROM {{ref('stg_tracks')}} as t
LEFT JOIN {{ref('int_users')}} USING (user_id)
