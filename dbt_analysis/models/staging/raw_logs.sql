{# Applying transformations to user_agent column #}

SELECT
  user_agent,
  CASE
    WHEN REGEXP_CONTAINS(user_agent, r'Chrome/') THEN 'Chrome'
    WHEN REGEXP_CONTAINS(user_agent, r'Opera/') THEN 'Opera'
    WHEN REGEXP_CONTAINS(user_agent, r'Firefox/') THEN 'Firefox'
    WHEN REGEXP_CONTAINS(user_agent, r'iPod|iPhone|bSafari/') THEN 'Mobile Safari'
    WHEN REGEXP_CONTAINS(user_agent, r'Safari/') THEN 'Safari'
    WHEN REGEXP_CONTAINS(user_agent, r'MSIE|Trident/') THEN 'Internet Explorer'
  END AS browser,

  REGEXP_EXTRACT(user_agent, r'([^\s/;]+)') AS browser,
  CASE
    WHEN REGEXP_CONTAINS(user_agent, r'Windows') THEN 'Windows'
    WHEN REGEXP_CONTAINS(user_agent, r'Macintosh|Mac OS X') THEN 'Mac OS'
    WHEN REGEXP_CONTAINS(user_agent, r'Linux') THEN 'Linux'
    WHEN REGEXP_CONTAINS(user_agent, r'iPod|iPhone|iPad') THEN 'iOS'
    WHEN REGEXP_CONTAINS(user_agent, r'Android') THEN 'Android'
    ELSE 'Other'
  END AS operating_system,
  CASE
    WHEN REGEXP_CONTAINS(user_agent, r'Windows|Macintosh|Linux') THEN 'Desktop'
    WHEN REGEXP_CONTAINS(user_agent, r'Mobile|Android|iPod|iPhone') THEN 'Mobile'
    WHEN REGEXP_CONTAINS(user_agent, r'iPad;') THEN 'Tablet'
    ELSE 'Other'
  END AS device_type
FROM
  `analytics-engineering-101.dbt_log_analytics.raw_logs` 
ORDER BY
  date DESC
;