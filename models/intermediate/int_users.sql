SELECT
    *,
    CASE
      WHEN REGEXP_CONTAINS(email, 'rightsnow|nicolas.dussart@gmail.com') THEN 'rightsnow'
      WHEN track_count = 0 THEN 'viewers'
      WHEN (broadcast_count / track_count) > 50 OR track_count > 25 THEN 'curious_users'
    ELSE
    'real_users'
  END
    AS user_category,
    SAFE_DIVIDE(broadcast_count, track_count) AS avg_broadcast
  FROM
    `rightsnow-385413.dbt_lnicolas.stg_users`