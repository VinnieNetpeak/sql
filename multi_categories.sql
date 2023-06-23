WITH
  sourse_table AS (
  SELECT
    DISTINCT(event_timestamp),
    event_date,
    event_name,
    user_pseudo_id,
    params.value.string_value AS category
  FROM
    `gtm-txdvwmd-ztyzm.analytics_288444991.events_*`,
    UNNEST(event_params) AS params
  WHERE
    event_name = 'click_button'
    AND params.key = 'button_title'),
  session_number AS(
  SELECT
    DISTINCT(event_timestamp),
    params.value.int_value AS session_number
  FROM
    `gtm-txdvwmd-ztyzm.analytics_288444991.events_*`,
    UNNEST(event_params) AS params
  WHERE
    event_name = 'click_button'
    AND params.key = 'ga_session_number'),
  category_session AS (
  SELECT
    sourse_table.event_timestamp,
    sourse_table.event_date,
    sourse_table.event_name,
    sourse_table.user_pseudo_id,
    sourse_table.category,
    session_number.session_number
  FROM
    sourse_table
  LEFT JOIN
    session_number
  ON
    sourse_table.event_timestamp = session_number.event_timestamp ),
  transactions_table AS(
  SELECT
    user_pseudo_id,
    params.value.int_value AS session_number,
    COUNT(event_name) AS transactions,
  FROM
    `gtm-txdvwmd-ztyzm.analytics_288444991.events_*`,
    UNNEST(event_params) AS params
  WHERE
    event_name = 'Lead'
    AND params.key = 'ga_session_number'
  GROUP BY
    user_pseudo_id,
    session_number),
  multicategory_table AS(
  SELECT
    STRING_AGG(category, ' > ') OVER (PARTITION BY user_pseudo_id ORDER BY session_number ROWS UNBOUNDED PRECEDING) AS multicategory,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, session_number ORDER BY category DESC) AS number,
    user_pseudo_id,
    session_number
  FROM
    category_session
  GROUP BY
    user_pseudo_id,
    session_number,
    category),
  max_multicategory AS(
  SELECT
    multicategory,
    user_pseudo_id,
    session_number
  FROM
    multicategory_table
  WHERE
    number = 1 )
SELECT
  max_multicategory.multicategory,
  SUM(transactions_table.transactions) AS transactions
FROM
  max_multicategory
LEFT JOIN
  transactions_table
ON
  max_multicategory.user_pseudo_id = transactions_table.user_pseudo_id
  AND max_multicategory.session_number = transactions_table.session_number
GROUP BY
  max_multicategory.multicategory
ORDER BY
  transactions DESC