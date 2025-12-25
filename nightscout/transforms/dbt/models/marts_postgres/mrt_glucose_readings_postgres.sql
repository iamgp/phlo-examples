{{ config(alias="mrt_glucose_readings") }}

select *
from {{ ref("mrt_glucose_readings") }}
