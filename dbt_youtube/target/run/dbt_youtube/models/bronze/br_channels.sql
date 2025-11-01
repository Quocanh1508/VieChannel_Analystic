
  create view "youtube_raw_db"."public"."br_channels__dbt_tmp"
    
    
  as (
    

select
  channel_id,
  title,
  "customUrl",
  "publishedAt"::timestamp as "PUBLISHED_AT",
  country,
  "subscriberCount"::bigint as "SUBSCRIBER_COUNT", -- SỬA LỖI Ở ĐÂY
  "viewCount"::bigint as "VIEW_COUNT",         -- Sửa luôn ở đây
  "videoCount"::bigint as "VIDEO_COUNT" ,        -- Sửa luôn ở đây
  thumbnail_high AS "CHANNEL_THUMBNAIL_URL"
FROM "youtube_raw_db"."public"."raw_channels"
  );