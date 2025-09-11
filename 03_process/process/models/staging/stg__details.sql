SELECT *
FROM
    {{ source('stemmingsuitslagen', 'details') }}
