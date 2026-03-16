{% test unique_combination(model, column_1, column_2) %}

-- Test fails if any combination of column_1 + column_2 appears more than once
-- dbt expects zero rows returned — any rows returned = duplicate found = test fails

SELECT
    {{ column_1 }},
    {{ column_2 }},
    COUNT(*) AS occurrences
FROM {{ model }}
GROUP BY {{ column_1 }}, {{ column_2 }}
HAVING COUNT(*) > 1

{% endtest %}