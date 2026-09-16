select
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    created_ts,
    batch_id as src_batch_id,
    source as source_system
from {{ source('bronze', 'raw_geolocation') }}
