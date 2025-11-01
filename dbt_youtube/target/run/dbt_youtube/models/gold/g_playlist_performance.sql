
  
    

  create  table "youtube_raw_db"."public"."g_playlist_performance__dbt_tmp"
  
  
    as
  
  (
    

with pi as (
  select
    playlist_id
    , count(distinct video_id) as items_detected
  from "youtube_raw_db"."public"."stg_videos"
  group by playlist_id
),
best as (
  select
    v.playlist_id
    , v.video_id
    -- SỬA LỖI: Dùng MIN (Postgres) thay vì any_value
    , MIN(v.video_title) as "VIDEO_TITLE"
    , MIN(v."VIEW_COUNT")  as "VIEW_COUNT"
    , row_number() over (partition by v.playlist_id order by MIN(v."VIEW_COUNT") desc) as rn
  from "youtube_raw_db"."public"."stg_videos" v
  group by v.playlist_id, v.video_id
)
select
  p.playlist_id
  , p.playlist_title                 as "PLAYLIST_TITLE"
  , p."ITEM_COUNT"                   as item_count_declared
  , pi.items_detected
  , p.channel_id
  , p.channel_title                  as "CHANNEL_TITLE"
  , p."PLAYLIST_THUMBNAIL_URL"     as playlist_thumbnail_url
  , b.video_id                       as top_video_id
  , b."VIDEO_TITLE"                  as top_video_title
  , b."VIEW_COUNT"                   as top_video_views
from "youtube_raw_db"."public"."stg_playlists" p
left join pi   on pi.playlist_id = p.playlist_id
left join best b on b.playlist_id = p.playlist_id and b.rn = 1
  );
  