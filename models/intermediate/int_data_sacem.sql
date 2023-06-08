-- Modify date source as first day of month 
SELECT 
date(cast(substring(date_date, 4, 6) as int64),cast(substring(date_date, 1, 3)as int64),1) as date_date
,num_repart
,id_sacem
,name_sacem
,type_droit
,val_sec
FROM {{ref('stg_data_sacem')}}
