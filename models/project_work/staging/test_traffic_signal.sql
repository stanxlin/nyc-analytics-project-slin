-- Quick test to verify source connection works
SELECT *
FROM {{ source('raw', 'source_traffic_signal_requests') }}
LIMIT 10