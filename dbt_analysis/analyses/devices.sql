{# Get device wise request type #}

SELECT
    device_type,
    COUNT(request_type) AS total_device_requests
FROM
    {{ ref('staging') }}
GROUP BY
    1
