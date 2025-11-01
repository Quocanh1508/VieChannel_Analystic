{{ config(materialized='table' ,
    tags=['gold']) }}

with ch as (
  select
    -- SỬA LỖI: Gọi các cột CHỮ HOA từ stg_channels
    "CHANNEL_ID",
    "TITLE" as "CHANNEL_TITLE",
    "CUSTOM_URL" as "CHANNEL_CUSTOM_URL",
    "PUBLISHED_AT" as "CHANNEL_PUBLISHED_AT",
    "COUNTRY",
    "SUBSCRIBER_COUNT",
    "CHANNEL_VIEW_COUNT",
    "CHANNEL_VIDEO_COUNT",
    "CHANNEL_THUMBNAIL_URL"
  from {{ ref('stg_channels') }}
),

v as (
  -- 1 dòng/video để tính Avg Length, Avg Rate
  select
    video_id
    -- SỬA LỖI: Dùng MIN (Postgres) thay vì any_value (Snowflake)
    -- SỬA LỖI: Gọi các cột CHỮ HOA từ stg_videos
    , MIN("DURATION_SECONDS") as "DURATION_SECONDS"
    , MIN("VIEW_COUNT")       as "VIEW_COUNT"
    , MIN("LIKE_COUNT")       as "LIKE_COUNT"
    , MIN("COMMENT_COUNT")    as "COMMENT_COUNT"
  from {{ ref('stg_videos') }}
  group by video_id
)

select
  ch.*,
  -- SỬA LỖI: Dùng các cột CHỮ HOA từ CTE 'v'
  avg(v."DURATION_SECONDS")                           as avg_video_length_sec,
  avg(v."LIKE_COUNT" / nullif(v."VIEW_COUNT", 0))     as avg_like_per_view,
  avg(v."COMMENT_COUNT" / nullif(v."VIEW_COUNT", 0))  as avg_comment_per_view
from ch
left join v on 1=1 -- cross join để lấy avg trên toàn bộ video
group by
  -- SỬA LỖI: Group by các cột CHỮ HOA từ CTE 'ch'
  ch."CHANNEL_ID", ch."CHANNEL_TITLE", ch."CHANNEL_CUSTOM_URL", ch."CHANNEL_PUBLISHED_AT", ch."COUNTRY",
  ch."SUBSCRIBER_COUNT", ch."CHANNEL_VIEW_COUNT", ch."CHANNEL_VIDEO_COUNT", ch."CHANNEL_THUMBNAIL_URL"