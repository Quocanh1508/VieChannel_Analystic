
  create view "youtube_raw_db"."public"."stg_channels__dbt_tmp"
    
    
  as (
    


select
    * 
from "youtube_raw_db"."public"."br_channels"
  );