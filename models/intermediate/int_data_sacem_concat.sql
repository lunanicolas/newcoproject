
-- Concatenate historical values and prediction TBD


SELECT 
date_date
,num_repart
,id_sacem
,histo.name_sacem
,presta_channel_id
,SUM(avg_value_sec) AS tot_val_sec

FROM {{ref('stg_data_sacem')}} as histo
LEFT JOIN {{source('projet_wagon', 'mapping_clean661')}} USING(id_sacem)
GROUP BY date_date
,num_repart
,id_sacem
,histo.name_sacem
,presta_channel_id
