--use july 23 value for all valuations
WITH 
    pred AS 
        (SELECT * FROM {{ref("int_data_sacem_concat")}} WHERE date_date = '2023-07-01'), 

-- get coef value for tv channels
    coef_tab AS 
        (SELECT
        monitoring_id,
        monitoring_date,
        sacem_channel_id,
        CASE
            WHEN 
                (EXTRACT(HOUR FROM monitoring_date) >= 23 OR EXTRACT(HOUR FROM monitoring_date) < 11) 
                AND sacem_channel_id IN (170, 117, 450, 455, 172, 113) 
                THEN 1
            WHEN 
                (EXTRACT(HOUR FROM monitoring_date) >= 11 AND EXTRACT(HOUR FROM monitoring_date) < 13)
                AND sacem_channel_id IN (170,117,450,455,172,113) 
                THEN 1.5
            WHEN 
                (EXTRACT(HOUR FROM monitoring_date) >= 13 AND EXTRACT(HOUR FROM monitoring_date) < 17) 
                AND sacem_channel_id IN (170, 117, 450, 455, 172, 113) 
                THEN 2
            WHEN 
                (EXTRACT(HOUR FROM monitoring_date) >= 17 AND EXTRACT(HOUR FROM monitoring_date) < 19)
                AND sacem_channel_id IN (170,117,450,455,172,113) 
                THEN 2
            WHEN 
                (EXTRACT(HOUR FROM monitoring_date) >= 19 AND EXTRACT(HOUR FROM monitoring_date) < 21) 
                AND sacem_channel_id IN (170, 117, 450, 455, 172, 113) OR (EXTRACT(HOUR FROM monitoring_date) = 21 
                AND EXTRACT(MINUTE FROM monitoring_date) < 30) AND sacem_channel_id IN (170, 117, 450, 455, 172, 113) 
                THEN 4
            WHEN 
                (EXTRACT(HOUR FROM monitoring_date) >= 21 AND EXTRACT(HOUR FROM monitoring_date) < 23) 
                AND sacem_channel_id IN (170,117,450,455,172,113) OR(EXTRACT(HOUR FROM monitoring_date) = 23 
                AND EXTRACT(MINUTE FROM monitoring_date) >= 0) AND sacem_channel_id IN (170,117,450,455,172,113) 
                THEN 2
            ELSE
                1
            END AS coef
    FROM {{ref('int_monitoring')}})

SELECT 
    m.monitoring_id,
    m.sacem_channel_id,
    sacem_channel_name,
    presta_channel_id,
    presta_channel_name,
    clean_channel_type,
    droits_sacem,
    m.monitoring_date,
    played_duration,
    m.track_id,
    artist,
    title,
    duration,
    track_upload_date,
    original_filename,
    t.user_id,
    email,
    first_name,
    last_name,
    broadcast_count,
    sub_date,
    track_count,
    user_category,
    track_category,
    coef,
    tot_val_sec,
    ROUND((played_duration * tot_val_sec * coef),2) as revenue_per_diffusion,
    forecast_lower,
    ROUND((played_duration * forecast_lower * coef),2) as lower_revenue_per_diffusion
    --SAFE_DIVIDE(broadcast_count, track_count) AS avg_broadcast
FROM {{ref('int_monitoring')}} as m
    LEFT JOIN  {{ref('int_tracks')}} as t USING (track_id) 
    LEFT JOIN {{ref('int_users')}} USING (user_id)
    LEFT JOIN pred ON pred.id_sacem = m.sacem_channel_id
    LEFT JOIN coef_tab USING (monitoring_id)