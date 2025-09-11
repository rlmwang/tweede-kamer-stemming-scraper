SELECT *
FROM
    {{ source('stemmingsuitslagen', 'motie') }}
