WITH sourse_table as (SELECT
  event_timestamp,
  event_name,
  user_pseudo_id,
  traffic_source.source,
  traffic_source.medium,
  params.value.int_value as session_number
FROM
  `sound-vault-327810.analytics_288444991.events_*`,
  UNNEST(event_params) as params
WHERE
  (event_name = "session_start"
  OR event_name = "Lead") AND params.key = "ga_session_number")

  SELECT * FROM sourse_table