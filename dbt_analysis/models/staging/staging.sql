{# Applying transformations to user_agent column #}

{{ 
    config(
        materialized='table',
        alias='staging_data'
    )
}}

WITH data_transformation AS (
    SELECT
        date,
        ip_address,
        user_agent,
        CASE
            WHEN REGEXP_CONTAINS(user_agent, r'Chrome/') THEN 'Chrome'
            WHEN REGEXP_CONTAINS(user_agent, r'Opera/') THEN 'Opera'
            WHEN REGEXP_CONTAINS(user_agent, r'Firefox/') THEN 'Firefox'
            WHEN REGEXP_CONTAINS(user_agent, r'iPod|iPhone|bSafari/') THEN 'Mobile Safari'
            WHEN REGEXP_CONTAINS(user_agent, r'Safari/') THEN 'Safari'
            WHEN REGEXP_CONTAINS(user_agent, r'MSIE|Trident/') THEN 'Internet Explorer'
        END AS browser,
        CASE
            WHEN REGEXP_CONTAINS(user_agent, r'Windows') THEN 'Windows'
            WHEN REGEXP_CONTAINS(user_agent, r'iPod|iPhone|iPad')
                AND NOT REGEXP_CONTAINS(user_agent, r'Macintosh|Mac OS X') THEN 'iOS'
            WHEN REGEXP_CONTAINS(user_agent, r'Macintosh|Mac OS X') THEN 'Mac OS'
            WHEN REGEXP_CONTAINS(user_agent, r'Linux') THEN 'Linux'
            WHEN REGEXP_CONTAINS(user_agent, r'Android') THEN 'Android'
            ELSE 'Other'
        END AS operating_system,
        CASE
            WHEN REGEXP_CONTAINS(user_agent, r'Windows|Macintosh|Linux') THEN 'Desktop'
            WHEN REGEXP_CONTAINS(user_agent, r'Mobile|Android|iPod|iPhone')
                AND NOT REGEXP_CONTAINS(user_agent, r'iPad') THEN 'Mobile'
            WHEN REGEXP_CONTAINS(user_agent, r'iPad') THEN 'Tablet'
            ELSE 'Other'
        END AS device_type,
        request_type,
        status_code,
        username
    FROM
        {{ source('log_data', 'raw_logs') }}
    ORDER BY
        date ASC
)

SELECT
    date,
    ip_address,
    user_agent,
    browser,
    operating_system,
    device_type,
    request_type,
    status_code,
    username
FROM
    data_transformation
