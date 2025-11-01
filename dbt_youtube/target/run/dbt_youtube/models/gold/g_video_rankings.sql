
  
    

  create  table "youtube_raw_db"."public"."g_video_rankings__dbt_tmp"
  
  
    as
  
  (
    

with v as (
  select
    video_id
    -- SỬA LỖI 1: Dùng MIN (Postgres) thay vì any_value (Snowflake)
    -- SỬA LỖI 2: Dùng tên cột CHỮ HOA (input từ stg_videos)
    , MIN(video_title)           as "VIDEO_TITLE"
    , MIN(video_desc)            as "VIDEO_DESC"
    , MIN("VIDEO_PUBLISHED_AT")  as "VIDEO_PUBLISHED_AT"
    , MIN("DURATION_SECONDS")    as "DURATION_SECONDS"
    , MIN("VIEW_COUNT")          as "VIEW_COUNT"
    , MIN("LIKE_COUNT")          as "LIKE_COUNT"
    , MIN("COMMENT_COUNT")       as "COMMENT_COUNT"
  from "youtube_raw_db"."public"."stg_videos"
  where video_title is not null
  group by video_id
)
select
  v.video_id
  , v."VIDEO_TITLE"
  , v."VIDEO_DESC"
  , v."VIDEO_PUBLISHED_AT"::date as published_date
  , v."DURATION_SECONDS"
  , v."VIEW_COUNT"
  , v."LIKE_COUNT"
  , v."COMMENT_COUNT"
  , v."LIKE_COUNT"   / nullif(v."VIEW_COUNT", 0) as like_per_view
  , v."COMMENT_COUNT"/ nullif(v."VIEW_COUNT", 0) as comment_per_view
  , row_number() over (order by v."VIEW_COUNT" desc)                       as rn_by_views
  , row_number() over (order by v."LIKE_COUNT"/nullif(v."VIEW_COUNT",0) desc)     as rn_by_like_rate
  , row_number() over (order by v."COMMENT_COUNT"/nullif(v."VIEW_COUNT",0) desc) as rn_by_comment_rate
  , row_number() over (order by v."VIDEO_PUBLISHED_AT" desc)                as rn_newest
from v
  );
  