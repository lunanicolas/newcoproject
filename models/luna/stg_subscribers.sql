SELECT day
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2022-03-12'), DATE('2023-06-03'), INTERVAL 1 DAY)
) as day