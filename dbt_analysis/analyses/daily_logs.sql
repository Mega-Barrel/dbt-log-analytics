
SELECT
    EXTRACT(DATE FROM date) AS dt,
    COUNT(*) AS daily_logs
FROM
    {{ ref('staging') }}
GROUP BY
    1
ORDER BY
    1 ASC
