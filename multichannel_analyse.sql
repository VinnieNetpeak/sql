WITH
  sourse_table AS (
  SELECT
    event_timestamp,
    event_name,
    user_pseudo_id,
    traffic_source.source,
    traffic_source.medium,
    params.value.int_value AS session_number
  FROM
    `sound-vault-327810.analytics_288444991.events_*`,
    UNNEST(event_params) AS params
  WHERE
    event_name = "session_start"
    AND params.key = "ga_session_number"),
  lead_table AS (
  SELECT
    sourse_table.event_timestamp,
    sourse_table.event_name,
    sourse_table.user_pseudo_id,
    CONCAT(sourse_table.source, ' / ',sourse_table.medium) as source_medium,
    sourse_table.session_number,
    GA.leads AS lead
  FROM
    sourse_table
  LEFT JOIN (
    SELECT
      user_pseudo_id,
      params.value.int_value AS session_number,
      COUNT(event_name) AS leads
    FROM
      `sound-vault-327810.analytics_288444991.events_*`,
      UNNEST(event_params) AS params
    WHERE
      params.key = 'ga_session_number'
      AND event_name ='Lead'
    GROUP BY
      user_pseudo_id,
      session_number) AS GA
  ON
    sourse_table.user_pseudo_id = GA.user_pseudo_id
    AND sourse_table.session_number = GA.session_number ),
    multichannel_analyse as (
        SELECT *,
         CASE WHEN session_number = 1 THEN 1
         ELSE 0 END as first_interaction,
         CASE WHEN lead > 0 THEN 1
         ELSE 0 END as lead_interaction,
         CASE WHEN session_number >1 and ifnull(lead, 0) = 0 THEN 1
         ELSE 0 END as assosiated_interaction
    
         from lead_table)
       

SELECT 	source_medium, SUM(first_interaction) as first_interaction, SUM(lead_interaction) as lead_interaction, SUM(assosiated_interaction) as assosiated_interaction FROM multichannel_analyse group by source_medium
