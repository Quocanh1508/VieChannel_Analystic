
  
    

  create  table "youtube_raw_db"."public"."g_channel_overview__dbt_tmp"
  
  
    as
  
  (
    

with ch as (
  select
    -- SỬA LỖI: Gọi các cột chữ thường từ stg_channels
    channel_id,
    title as "CHANNEL_TITLE",
    "customUrl" as "CHANNEL_CUSTOM_URL",
    published_at as "CHANNEL_PUBLISHED_AT",
    country,
    subscriber_count,
    view_count AS "CHANNEL_VIEW_COUNT",
    video_count AS "CHANNEL_VIDEO_COUNT",
    channel_thumbnail_url 
  from "youtube_raw_db"."public"."stg_channels"
),
v as (
  -- 1 dòng/video để tính Avg Length, Avg Rate
  select
    video_id
    -- SỬA LỖI: Dùng MIN (Postgres) và cột CHỮ HOA (từ stg_videos)
    , MIN("DURATION_SECONDS") as "DURATION_SECONDS"
    , MIN("VIEW_COUNT")       as "VIEW_COUNT"
    , MIN("LIKE_COUNT")       as "LIKE_COUNT"
    , MIN("COMMENT_COUNT")    as "COMMENT_COUNT"
  from "youtube_raw_db"."public"."stg_videos"
  group by video_id
)
select
  ch.*,
  avg(v."DURATION_SECONDS")                           as avg_video_length_sec,
  avg(v."LIKE_COUNT" / nullif(v."VIEW_COUNT", 0))     as avg_like_per_view,
  avg(v."COMMENT_COUNT" / nullif(v."VIEW_COUNT", 0))  as avg_comment_per_view
from ch
left join v on 1=1 -- cross join để lấy avg trên toàn bộ video
group by
  -- SỬA LỖI: Group by các cột chữ thường từ CTE 'ch'
  ch.channel_id, ch."CHANNEL_TITLE", ch."CHANNEL_CUSTOM_URL", ch."CHANNEL_PUBLISHED_AT", ch.country,
  ch.subscriber_count, ch."CHANNEL_VIEW_COUNT", ch."CHANNEL_VIDEO_COUNT", ch.channel_thumbnail_url
  );
  