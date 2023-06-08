WITH
  l AS(
  SELECT
    user_id,
    MAX(login_date) AS last_login_date
  FROM
    {{ref('stg_logs')}}
  WHERE
    login_successful = 1
  GROUP BY
    user_id),
  t AS(
  SELECT
    user_id,
    MIN(track_id) AS first_upload_track_id
  FROM
  {{ref('stg_tracks')}}
  GROUP BY
    user_id )
SELECT
  u.user_id,
  email,
  first_name,
  last_name,
  broadcast_count,
  verified,
  archived,
  sub_date,
  track_count,
  t.first_upload_track_id,
  tracks.creation_ts AS upload_date,
  l.last_login_date,
  --last_modification_ts,
  CASE
    WHEN REGEXP_CONTAINS(email, 'rightsnow|nicolas.dussart@gmail.com') THEN 'rightsnow'
    WHEN verified IS NULL THEN 'non_verified_account'
    WHEN archived = 1 THEN 'deleted_account'
    WHEN track_count = 0 THEN 'viewer'
    WHEN (tracks.creation_ts IS NOT NULL AND l.last_login_date IS NULL) OR (tracks.creation_ts >= l.last_login_date) THEN 'curious_user'
  ELSE
  'real_user'
END
  AS user_category
FROM
 {{ref('stg_users')}} AS u
LEFT JOIN t USING (user_id)
LEFT JOIN {{ref('stg_tracks')}} AS tracks ON t.first_upload_track_id = tracks.track_id
LEFT JOIN l ON u.user_id = l.user_id