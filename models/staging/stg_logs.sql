SELECT 
user as user_id
,login_date
,login_successful

FROM {{ source('projet_wagon', 'logs') }}