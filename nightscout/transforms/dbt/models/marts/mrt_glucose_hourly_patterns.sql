-- mrt_glucose_hourly_patterns.sql - Mart for hourly glucose pattern analysis
-- Aggregates glucose readings by hour and day of week for time-of-day trend analysis
-- Enables identification of patterns like dawn phenomenon and post-meal spikes

{{ config(
   materialized='table',
    tags=['nightscout', 'mart']
) }}

/*
Hourly glucose patterns for time-of-day analysis

This mart aggregates glucose readings by hour of day to identify patterns
like dawn phenomenon, post-meal spikes, and overnight trends.

Target: Iceberg marts schema
Use case: Heatmaps and time-of-day pattern analysis in BI tools
*/

with date_params as (
    select
        {% if var('partition_date_str', none) is not none %}
        timestamp '{{ var('partition_date_str') }}' as anchor_ts
        {% else %}
        current_timestamp as anchor_ts
        {% endif %}
)

-- Select statement: Aggregate glucose metrics by hour and day of week
select
    hour_of_day,
    day_of_week,
    day_name,

    count(*) as reading_count,
    round(avg(glucose_mg_dl), 1) as avg_glucose_mg_dl,
    round(approx_percentile(glucose_mg_dl, 0.5), 1) as median_glucose_mg_dl,
    round(approx_percentile(glucose_mg_dl, 0.25), 1) as p25_glucose_mg_dl,
    round(approx_percentile(glucose_mg_dl, 0.75), 1) as p75_glucose_mg_dl,

    -- Time in range for this hour/day combination
    round(100.0 * sum(is_in_range) / count(*), 1) as time_in_range_pct,

    -- Variability
    round(stddev(glucose_mg_dl), 1) as stddev_glucose_mg_dl

from {{ ref('mrt_glucose_readings') }}
cross join date_params
where
    reading_timestamp >= date_params.anchor_ts - interval '30' day
    and reading_timestamp < date_params.anchor_ts + interval '1' day
group by hour_of_day, day_of_week, day_name
order by day_of_week, hour_of_day
