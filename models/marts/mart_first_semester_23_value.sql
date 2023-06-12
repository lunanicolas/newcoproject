
SELECT
    user_id, 
    user_category, 
    EXTRACT(MONTH FROM monitoring_date) as broadcast_month, 
    EXTRACT(YEAR FROM monitoring_date) as broadcast_year, 
    ROUND(SUM(revenue_per_diffusion),2) AS revenue
  FROM {{ref('mart_revenue_per_diffusion')}}
  WHERE user_category != 'rightsnow' AND DATE(monitoring_date) BETWEEN '2023-01-01' AND '2023-06-30' AND track_category != 'test_track'
  GROUP BY user_id, email, user_category, EXTRACT(MONTH FROM monitoring_date), EXTRACT(YEAR FROM monitoring_date)

