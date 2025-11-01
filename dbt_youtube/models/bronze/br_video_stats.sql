{{ config(materialized='view' ,
    tags=['bronze']) }}


with base as (
  select
    video_id
    , video_title
    , video_desc
    
    -- SỬA LỖI 1: dùng CAST chuẩn của SQL/Postgres thay vì hàm try_to_timestamp_ntz của Snowflake
    , CAST(video_published_at AS timestamp) as "VIDEO_PUBLISHED_AT"
    , duration_iso8601

    -- SỬA LỖI 2: dùng CAST chuẩn của SQL/Postgres thay vì hàm try_to_number của Snowflake
    , CAST(view_count AS numeric) as "VIEW_COUNT"
    , CAST(like_count AS numeric) as "LIKE_COUNT"
    , CAST(comment_count AS numeric) as "COMMENT_COUNT"

    , CAST(live_actual_start AS timestamp) as "LIVE_ACTUAL_START"
    , CAST(live_actual_end AS timestamp)   as "LIVE_ACTUAL_END"
    , CAST(live_scheduled_start AS timestamp) as "LIVE_SCHEDULED_START"
  FROM {{ source('raw_postgres_data', 'raw_video_stats') }}
),

parsed as (
  select
    *,
    
    -- SỬA LỖI 3: dùng hàm substring(... FROM ...) của Postgres thay vì regexp_substr của Snowflake
    -- và dùng CAST thay vì to_number
      coalesce(CAST(substring(duration_iso8601 FROM '(\d+)H') AS numeric), 0) * 3600
    + coalesce(CAST(substring(duration_iso8601 FROM '(\d+)M') AS numeric), 0) * 60
    + coalesce(CAST(substring(duration_iso8601 FROM '(\d+)S') AS numeric), 0)
      as "DURATION_SECONDS"
      
  from base
)

select * from parsed