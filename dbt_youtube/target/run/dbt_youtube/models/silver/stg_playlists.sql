
  create view "youtube_raw_db"."public"."stg_playlists__dbt_tmp"
    
    
  as (
    

select
  p.*
  , c.title as channel_title
from "youtube_raw_db"."public"."br_playlists" p
left join "youtube_raw_db"."public"."br_channels" c
  on p.channel_id = c.channel_id
  );