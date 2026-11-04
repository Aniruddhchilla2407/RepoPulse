{% test no_future_timestamps(model, column_name, tolerance_hours=24) %}

-- Fails for every row whose timestamp is more than `tolerance_hours` in the future.
select {{ column_name }} as bad_value
from {{ model }}
where {{ column_name }} > cast(now() as timestamp) + interval {{ tolerance_hours }} hour

{% endtest %}