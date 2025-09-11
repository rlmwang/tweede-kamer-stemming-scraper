SELECT *
FROM
    {{ source('stemmingsuitslagen', 'indieners') }}
