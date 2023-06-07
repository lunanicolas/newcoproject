SELECT
    id AS user_id,
    LOWER(email) AS email,
    LOWER(first_name) AS first_name,
    LOWER(last_name) AS last_name,
    track_count,
    broadcast_count,
    --active,
    verified,
    archived,
    Date_d_inscription AS sub_date,
    last_modification_ts
  FROM
    {{ source('projet_wagon', 'histo_sacem') }}