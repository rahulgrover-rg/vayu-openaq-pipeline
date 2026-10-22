select sensor_id, valid_from, valid_to, reason
from {{ ref('quality_overrides') }} 
where valid_from > valid_to