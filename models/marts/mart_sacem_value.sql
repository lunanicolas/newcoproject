WITH sacem AS (SELECT DISTINCT *
FROM {{ref('int_data_sacem')}})

SELECT date_date
,num_repart
,id_sacem
,name_sacem
,type_droit
,AVG(val_sec) as avg_value_sec
FROM sacem
GROUP BY date_date 
,num_repart
,id_sacem
,name_sacem
,type_droit