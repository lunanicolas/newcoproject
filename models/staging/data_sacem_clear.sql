SELECT 
CONCAT(SUBSTRING(date_source,4,3),SUBSTRING(date_source,7,4)) AS date_date,
num_repart,
id_sacem,
name_sacem,
type_droit,
val_sec
FROM `rightsnow-385413.projet_wagon.histo_sacem`