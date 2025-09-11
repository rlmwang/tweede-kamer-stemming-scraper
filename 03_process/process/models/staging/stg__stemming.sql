SELECT *
FROM
    {{ source('stemmingsuitslagen', 'stemming') }}
