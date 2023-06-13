-- get unique values for historical values by calculating average and modify date format
WITH sacem AS (SELECT DISTINCT 
date(cast(substring(date_source, 7, 4) as int64),cast(substring(date_source, 5, 2)as int64),1)  AS date_date
,num_repart
,id_sacem
,name_sacem
,type_droit
,val_sec
FROM {{ source('projet_wagon', 'histo_sacem')}})


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