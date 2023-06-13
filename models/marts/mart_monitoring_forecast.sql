SELECT
  monitoring_id,
  monitoring_date,
  presta_channel_name,
  artist,
  title,
  played_duration,
  user_id,
  broadcast_count,
  user_category,
  track_category,
  clean_channel_type,
  revenue_per_diffusion,
  lower_revenue_per_diffusion
FROM {{ref("mart_master_monitoring")}} 