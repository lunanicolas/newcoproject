with new_customers AS (select 
DATE(sub_date) as sub_date
,count(*) as new_customers
from {{ref("int_users")}}
where user_category NOT IN( 'rightsnow', 'non_verified_account')
group by DATE(sub_date))

SELECT day
, IFNULL(new_customers, 0) as new_customers
, sum(new_customers) OVER(ORDER BY day) AS cumulative_base
FROM {{ref('stg_subscribers')}} as s

LEFT JOIN new_customers as n ON s.day = n.sub_date
