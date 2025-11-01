{{ config(materialized='view' ,
    tags=['silver']) }}

select
  -- SỬA LỖI: Các cột này đến từ 'br_playlist_items' (phải dùng CHỮ HOA)
  pi."PLAYLIST_ITEM_ID",
  pi.playlist_id,
  pi.channel_id,
  pi.video_id,

  -- SỬA LỖI: Các cột này đến từ 'br_video_stats' (phải dùng CHỮ HOA hoặc chữ thường)
  s.video_title,
  s.video_desc,
  s."VIDEO_PUBLISHED_AT" as "VIDEO_PUBLISHED_AT",
  s."VIEW_COUNT" as "VIEW_COUNT",
  s."LIKE_COUNT" as "LIKE_COUNT",
  s."COMMENT_COUNT" as "COMMENT_COUNT",
  s."DURATION_SECONDS" as "DURATION_SECONDS",
  
  -- Hàm này đã sửa đúng (dùng cột CHỮ HOA)
  (current_timestamp::date - s."VIDEO_PUBLISHED_AT"::date) as "VIDEO_AGE_DAYS",
  
  CASE 
        -- SỬA LỖI: Dùng cột CHỮ HOA
        WHEN s."LIVE_SCHEDULED_START" is not null THEN 'Live'
        WHEN s."DURATION_SECONDS" < 60 THEN 'Shorts'
        WHEN s."DURATION_SECONDS" is null THEN 'Private videos'
        ELSE 'Normal videos'
    END AS "VIDEO_TYPE",
  
  CASE
      -- SỬA LỖI: Dùng cột CHỮ HOA
      WHEN s."DURATION_SECONDS" <   60 THEN '0-1m'
      WHEN s."DURATION_SECONDS" <  300 THEN '1-5m'
      WHEN s."DURATION_SECONDS" <  900 THEN '5-15m'
      WHEN s."DURATION_SECONDS" < 1800 THEN '15-30m'
      WHEN s."DURATION_SECONDS" < 3600 THEN '30-60m'
      ELSE '60m+'
    END AS "DURATION_BUCKET",

  -- SỬA LỖI: Các cột này đến từ 'br_playlist_items' (phải dùng CHỮ HOA)
  pi."ADDED_TO_PLAYLIST_AT" AS "VIDEO_ADDED_TO_PLAYLIST_AT",
  pi."POSITION_IN_PLAYLIST" AS "VIDEO_POSITION_IN_PLAYLIST",
  pi."VIDEO_THUMBNAIL_URL_IN_PLAYLIST" AS "VIDEO_THUMBNAIL_URL"

from {{ ref('br_playlist_items') }} pi
left join {{ ref('br_video_stats') }} s
  -- Join key là chữ thường ở cả 2 file, nên giữ nguyên
  on pi.video_id = s.video_id