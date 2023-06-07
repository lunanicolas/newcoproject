SELECT
    *,
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
    {{ref('stg_tracks')}}
