WITH
  first_visit AS (
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS cohort_date,
    user_pseudo_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    event_name = 'first_visit'),
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
  date_difference AS (
  SELECT
    DATE_DIFF(DATE(TIMESTAMP_MICROS(GA.event_timestamp)), first_visit.cohort_date, MONTH) AS month_number,
    GA.user_pseudo_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` GA
  LEFT JOIN
    first_visit
  ON
    GA.user_pseudo_id = first_visit.user_pseudo_id,
    UNNEST(GA.event_params) AS params
  WHERE
    GA.event_name = 'session_start'
    AND params.value.int_value > 1
  GROUP BY
    2,
    1),
  retention AS (
  SELECT
    EXTRACT(month
    FROM
      first_visit.cohort_date) AS cohort_month,
    date_difference.month_number,
    COUNT(1) AS num_users
  FROM
    date_difference
  LEFT JOIN
    first_visit
  ON
    date_difference.user_pseudo_id = first_visit.user_pseudo_id
  GROUP BY
    1,
    2 )
SELECT
  *
FROM
  retention where month_number >= 0
LIMIT
  1000