{{ config(materialized='view', schema='silver') }}

SELECT file_version
FROM {{ source('bronze', 'raw_stops') }}
WHERE NULLIF(TRIM(file_version), '') IS NOT NULL
GROUP BY file_version
ORDER BY MAX(loaded_at) DESC, file_version DESC
LIMIT 1
