-- Найти группы дубликатов
WITH row_data AS (
    SELECT * FROM {{ ref('cur_nanomag') }}  -- Используйте ref() вместо жесткого пути
),

row_hashes AS (
    SELECT
        *,
        MD5(CAST(row_to_json(t) AS VARCHAR)) AS row_hash
    FROM
        row_data t
),

duplicate_groups AS (
    SELECT
        row_hash,
        COUNT(*) AS group_size
    FROM
        row_hashes
    GROUP BY
        row_hash
    HAVING
        COUNT(*) > 1
)

-- Показать дубликаты
SELECT
    rh.*,
    dg.group_size,
    ROW_NUMBER() OVER (PARTITION BY rh.row_hash ORDER BY rh.nanoparticle) AS duplicate_number
FROM
    row_hashes rh
JOIN
    duplicate_groups dg ON rh.row_hash = dg.row_hash
ORDER BY
    rh.row_hash, duplicate_number
