
  
    

  create  table "youtube_raw_db"."public"."g_duration_distribution__dbt_tmp"
  
  
    as
  
  (
    

with v as (
  select
    video_id
    -- SỬA LỖI: Dùng MIN và cột CHỮ HOA
    , MIN("DURATION_SECONDS") as "DURATION_SECONDS"
    , MIN(case
        when "DURATION_SECONDS" <   60 then '0-1m'
        when "DURATION_SECONDS" <  300 then '1-5m'
        when "DURATION_SECONDS" <  900 then '5-15m'
        when "DURATION_SECONDS" < 1800 then '15-30m'
        when "DURATION_SECONDS" < 3600 then '30-60m'
        else '60m+'
      end) as "DURATION_BUCKET"
  from "youtube_raw_db"."public"."stg_videos"
  group by video_id
)
select
  "DURATION_BUCKET" as duration_bucket
  , count(*)        as videos_count
  , avg("DURATION_SECONDS") as avg_duration_sec
from v
group by 1
order by
  -- SỬA LỖI: Dùng đúng bí danh CHỮ HOA
  case "DURATION_BUCKET"
    when '0-1m'   then 1
    when '1-5m'   then 2
    when '5-15m'  then 3
    when '15-30m' then 4
    when '30-60m' then 5
    else 6
  end
  );
  