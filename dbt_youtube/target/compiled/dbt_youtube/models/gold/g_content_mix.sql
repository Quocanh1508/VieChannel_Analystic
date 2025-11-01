

with v as (
  select
    video_id
    -- SỬA LỖI: Dùng MIN (Postgres) và cột CHỮ HOA
    , MIN("VIEW_COUNT")    as "VIEW_COUNT"
    , MIN("LIKE_COUNT")    as "LIKE_COUNT"
    , MIN("COMMENT_COUNT") as "COMMENT_COUNT"
    , MIN("VIDEO_TYPE")    as "VIDEO_TYPE"
  from "youtube_raw_db"."public"."stg_videos"
  group by video_id
)
select
  "VIDEO_TYPE" as video_type
  , count(*)   as videos_count
  , sum("VIEW_COUNT") as total_views
  , avg("LIKE_COUNT"   / nullif("VIEW_COUNT",0))     as avg_like_per_view
  , avg("COMMENT_COUNT"/ nullif("VIEW_COUNT",0))     as avg_comment_per_view
from v
group by 1
order by videos_count desc