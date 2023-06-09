SELECT 
id as track_id
,artist
,title
,duration
--,acr_id as presta_id
,user as user_id
--,active
--,archived
,creation_ts
--,last_modification_ts
,original_filename
FROM {{ source('projet_wagon', 'tracks') }}
