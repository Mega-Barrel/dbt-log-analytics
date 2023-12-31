{# Browser wise status code #}

SELECT
  browser,
  status_code,
  COUNT(status_code) AS total_browser_requests
FROM
  {{ ref('staging') }}
GROUP BY
  1, 2
ORDER BY
  1
