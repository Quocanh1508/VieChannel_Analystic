{{ config(materialized='table' ,
    tags=['gold']) }}

with v as (
  select
    video_id
    -- SỬA LỖI 1 & 2: Dùng MIN (Postgres) VÀ cột CHỮ HOA
    , MIN("VIDEO_PUBLISHED_AT") as "VIDEO_PUBLISHED_AT"
  from {{ ref('stg_videos') }}
  -- SỬA LỖI 2: Dùng cột CHỮ HOA
  where "VIDEO_PUBLISHED_AT" is not null
  group by video_id
)
select
  extract(dow  from "VIDEO_PUBLISHED_AT") as published_dow   -- 0=Sun
  , extract(hour from "VIDEO_PUBLISHED_AT") as published_hour -- 0..23
  , count(*) as videos_count
from v
group by 1,2