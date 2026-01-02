{{
    config(
        materialized='table',
        schema='silver'
    )
}}

/*
  Staged Pokemon abilities
  - Extracts ability ID from URL
  - Cleans names
*/

WITH source AS (
    SELECT * FROM {{ source('dagster_assets', 'pokemon_abilities') }}
),

staged AS (
    SELECT
        CAST(
            REGEXP_EXTRACT(url, '/ability/(\d+)/', 1) AS INTEGER
        ) AS ability_id,
        LOWER(TRIM(name)) AS ability_name,
        REPLACE(name, '-', ' ') AS ability_display_name,
        url AS api_url,
        _phlo_ingested_at
    FROM source
    WHERE name IS NOT NULL
)

SELECT * FROM staged
