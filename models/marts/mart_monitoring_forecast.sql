SELECT
  monitoring_id,
  monitoring_date,
  channel_name,
  artist,
  title,
  duration,
  user_id,
  broadcast_count,
  user_category,
  track_category,
  type,
  tot_val_sec,
  revenue_per_diffusion,
  forecast.forecast,
  ROUND((forecast.forecast * duration),8) AS forecast_revenue_per_diffusion,
  forecast.forecast_lower,
  ROUND((forecast.forecast_lower * duration),8) AS forecastlower_revenue_per_diffusion
FROM
  {{ref("mart_revenue_per_diffusion")}} revenue
JOIN
  {{ref("mart_forecast_per_channel")}} forecast
ON forecast.channel_name_presta = revenue.channel_name