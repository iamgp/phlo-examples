-- mrt_glucose_overview.sql - Glucose overview mart for BI dashboards in PostgreSQL
-- Incrementally materialized mart providing denormalized daily glucose metrics
-- Optimized for fast dashboard queries and visualization in Superset

{{ config(
    materialized='table',
    tags=['nightscout', 'mart']
) }}

/*
Glucose overview mart for BI dashboards

This mart table is materialized in PostgreSQL for fast dashboard queries in
Superset. It provides a denormalized, aggregated view optimized for
visualization and reporting.

Target: PostgreSQL marts schema
Refresh: Every 30 minutes via Dagster schedule
*/

-- Select statement: Enrich daily glucose data with rolling averages and trend indicators
with source_data as (
    select * from {{ ref('fct_daily_glucose_metrics') }}
),
date_params as (
    select
        {% if var('partition_date_str', none) is not none %}
        date('{{ var('partition_date_str') }}') as anchor_date
        {% else %}
        current_date as anchor_date
        {% endif %}
)

select
    source_data.reading_date,
    source_data.day_name,
    source_data.week_of_year,
    source_data.month,
    source_data.year,

    -- Daily metrics
    source_data.reading_count,
    source_data.avg_glucose_mg_dl,
    source_data.min_glucose_mg_dl,
    source_data.max_glucose_mg_dl,
    source_data.stddev_glucose_mg_dl,

    -- Time in range (key diabetes management metric)
    source_data.time_in_range_pct,
    source_data.time_below_range_pct,
    source_data.time_above_range_pct,

    -- Estimated HbA1c (7-day rolling average)
    source_data.estimated_a1c_pct,
    avg(source_data.estimated_a1c_pct) over (
        order by source_data.reading_date
        rows between 6 preceding and current row
    ) as estimated_a1c_7d_avg,

    -- Trend indicators
    source_data.avg_glucose_mg_dl - lag(source_data.avg_glucose_mg_dl) over (
        order by source_data.reading_date
    ) as glucose_change_from_prev_day,

    -- Glucose variability coefficient
    case
        when source_data.avg_glucose_mg_dl > 0
            then round(
                100.0 * source_data.stddev_glucose_mg_dl / source_data.avg_glucose_mg_dl,
                1
            )
    end as coefficient_of_variation

from source_data
cross join date_params
where
    source_data.reading_date >= date_params.anchor_date - interval '90' day
    and source_data.reading_date <= date_params.anchor_date

order by source_data.reading_date desc
