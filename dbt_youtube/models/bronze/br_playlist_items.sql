{{ config(materialized='view' ,
    tags=['bronze']) }}


select
  "playlistItem_id" AS "PLAYLIST_ITEM_ID", 
  playlist_id,
  video_id,
  video_title as "VIDEO_TITLE_IN_PLAYLIST",
  video_desc as "VIDEO_DESC_IN_PLAYLIST",
  
  CAST(added_to_playlist_at AS timestamp) as "ADDED_TO_PLAYLIST_AT",
  CAST(video_published_at AS timestamp) as "VIDEO_PUBLISHED_AT",
  CAST(position_in_playlist AS numeric) as "POSITION_IN_PLAYLIST",
  
  snippet__thumbnails__maxres__url AS "VIDEO_THUMBNAIL_URL_IN_PLAYLIST",
  channel_id
  
FROM {{ source('raw_postgres_data', 'raw_playlist_items') }}