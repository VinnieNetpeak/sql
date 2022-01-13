WITH
  --таблица первых посещений с датами и client_id
  first_visit AS (
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS cohort_date,
    user_pseudo_id
  FROM
    `sound-vault-327810.analytics_288444991.events_*`
  WHERE
    event_name = 'first_visit'),

  --таблица с общим количеством пользователей, которые посетили сайт 
  --первый раз по месяцам
  count_first_visit AS (
  SELECT
    EXTRACT(month
    FROM
      cohort_date) AS month,
    COUNT(user_pseudo_id) AS users
  FROM
    first_visit
  GROUP BY
    month),
  
  --таблица с разницой посещений пользователей по месяцам
  date_difference AS (
  SELECT
    DATE_DIFF(DATE(TIMESTAMP_MICROS(GA.event_timestamp)), first_visit.cohort_date, MONTH) AS month_number,
    GA.user_pseudo_id
  FROM
    `sound-vault-327810.analytics_288444991.events_*` GA
  LEFT JOIN
    first_visit
  ON
    GA.user_pseudo_id = first_visit.user_pseudo_id,
    UNNEST(GA.event_params) AS params
  WHERE
    GA.event_name = 'session_start'
  GROUP BY
    2,
    1),
  
  --таблица c годом, номером месяца, месяцем, номером месяца возврата
  retention AS (
  SELECT
    EXTRACT(year
    FROM
      first_visit.cohort_date) AS cohort_year,
    EXTRACT(month
    FROM
      first_visit.cohort_date) AS cohort_month_num,
    FORMAT_DATE("%B", first_visit.cohort_date) AS cohort_month,
    CASE
      WHEN date_difference.month_number = 0 THEN 'Month_0'
      WHEN date_difference.month_number = 1 THEN 'Month_1'
      WHEN date_difference.month_number = 2 THEN 'Month_2'
      WHEN date_difference.month_number = 3 THEN 'Month_3'
      WHEN date_difference.month_number = 4 THEN 'Month_4'
      WHEN date_difference.month_number = 5 THEN 'Month_5'
      WHEN date_difference.month_number = 6 THEN 'Month_6'
      WHEN date_difference.month_number = 7 THEN 'Month_7'
      WHEN date_difference.month_number = 8 THEN 'Month_8'
      WHEN date_difference.month_number = 9 THEN 'Month_9'
      WHEN date_difference.month_number = 10 THEN 'Month_10'
      WHEN date_difference.month_number = 11 THEN 'Month_11'
      WHEN date_difference.month_number = 12 THEN 'Month_12'
    ELSE
    'Month_er'
  END
    AS month_number,
    COUNT(first_visit.user_pseudo_id) AS num_users
  FROM
    date_difference
  LEFT JOIN
    first_visit
  ON
    date_difference.user_pseudo_id = first_visit.user_pseudo_id
  GROUP BY
    1,
    2,
    3,
    4 ),
  
  --итоговая таблица куда еще добавляется количество всех пользователей в 
  --в месяц 0 для дальнейшего расчета процента возврата.
  final_table AS (
  SELECT
    *,
    FIRST_VALUE(num_users) OVER(PARTITION BY cohort_month ORDER BY month_number) AS Month_0
  FROM
    retention)
SELECT
  cohort_year,
  CONCAT(cohort_month_num, "_", cohort_month) as Month,
  month_number,
  num_users,
  Month_0
FROM
  final_table
WHERE
  month_number != 'Month_er'
ORDER BY
  cohort_year,
  cohort_month DESC,
  month_number
LIMIT
  1000