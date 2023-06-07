WITH l AS(
  SELECT
    user_id,
    MAX(login_date) AS last_login_date
  FROM {{ref('stg_logs')}}
  WHERE login_successful = 1
  GROUP BY user_id),
      
      t AS(
  SELECT
    user_id,
    MIN(track_id) AS first_upload_track_id
  FROM {{ref('stg_tracks')}}
  GROUP BY user_id
    )

SELECT
    user_id,
    email,
    first_name,
    last_name,
    broadcast_count,
    verified,
    archived,
    sub_date,
    track_count,
    t.first_upload_track_id,
    track_id,
    tracks.creation_ts AS upload_date,
    l.last_login_date,
    --last_modification_ts,
    CASE
      WHEN REGEXP_CONTAINS(email, 'rightsnow|nicolas.dussart@gmail.com') THEN 'rightsnow'
      WHEN verified is null then 'non_verified_account'
      WHEN archived = 1 THEN 'deleted_account'
      WHEN track_count = 0 THEN 'viewers'
      WHEN (broadcast_count / track_count) > 50 OR track_count > 25 THEN 'curious_users'
    ELSE
    'real_users'
  END
    AS user_category,
    SAFE_DIVIDE(broadcast_count, track_count) AS avg_broadcast
  FROM
    {{ref('stg_users')}} AS u
LEFT JOIN t
USING(user_id)
LEFT JOIN {{ref('stg_tracks')}} AS tracks
USING(user_id)
LEFT JOIN l
USING(user_id)