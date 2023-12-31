{# Get total devices running various OS #}

SELECT
  operating_system,
  COUNT(operating_system) AS total_os_requests
FROM
  {{ ref('staging') }}
GROUP BY
  1
ORDER BY
  1
