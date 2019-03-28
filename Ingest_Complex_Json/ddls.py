from pyspark.sql.types import *

user_tbl_json_ingest_schema = StructType() \
    .add("apps", ArrayType(
    StructType()
        .add("first_used", StringType())
        .add("last_used", StringType())
        .add("name", StringType())
        .add("platform", StringType())
        .add("sessions", LongType())
        .add("version", StringType())
)
         ) \
    .add("system_id", StringType()) \
    .add("campaigns_received", ArrayType(
    StructType()
        .add("api_campaign_id", StringType())
        .add("converted", BooleanType())
        .add("engaged", StructType()
             .add("clicked_email", BooleanType())
             .add("clicked_triggered_in_app_message", BooleanType())
             .add("opened_email", BooleanType())
             .add("open_push", BooleanType())
             )
        .add("in_control", BooleanType())
        .add("last_received", StringType())
        .add("name", StringType())
        .add("variation_api_id", StringType())
        .add("variation_name", StringType())
)) \
    .add("canvases_received", ArrayType(
    StructType()
        .add("api_canvas_id", StringType())
        .add("in_control", StringType())
        .add("is_in_control", BooleanType())
        .add("last_entered", StringType())
        .add("last_entered_control_at", StringType())
        .add("last_exited", StringType())
        .add("last_received_message", StringType())
        .add("name", StringType())
        .add("steps_received", ArrayType(
        StructType()
            .add("api_canvas_step_id", StringType())
            .add("last_received", StringType())
            .add("name", StringType())
    ))
        .add("variation_name", StringType())
)) \
    .add("country", StringType()) \
    .add("created_at", StringType()) \
    .add("custom_attributes", StructType()
         .add("active_flag", StringType())   ## better cast to StringType if have any doubt on the consistency of JSON data types here
         .add("date_created", StringType())
         .add("email_deliverability_status", StringType())
         .add("email_permission_status", StringType())
         .add("is_corporate", StringType())
         .add("last_email_click_date", StringType())
         .add("last_email_delivered_date", StringType())
         .add("last_email_open_date", StringType())
         .add("last_order_date", StringType())
         .add("responsys_last_open_date", StringType())
         .add("source_first", StringType())
         .add("source_recent", StringType())
         .add("zip_code", StringType())
         ) \
    .add("custom_events", ArrayType(
    StructType()
        .add("count", LongType())
        .add("first", StringType())
        .add("last", StringType())
        .add("name", StringType())
)) \
    .add("devices", ArrayType(
    StructType()
        .add("ad_tracking_enabled", StringType())
        .add("career", StringType())
        .add("device_id", StringType())
        .add("idfa", StringType())
        .add("idfv", StringType())
        .add("model", StringType())
        .add("os", StringType())
)) \
    .add("email", StringType()) \
    .add("email_subscribe", StringType()) \
    .add("email_unsubscribed_at", StringType()) \
    .add("external_id", StringType()) \
    .add("first_name", StringType()) \
    .add("gender", StringType()) \
    .add("language", StringType()) \
    .add("last_name", StringType()) \
    .add("purchases", ArrayType(
    StructType()
        .add("count", LongType())
        .add("first", StringType())
        .add("last", StringType())
        .add("name", StringType())
)) \
    .add("push_opted_in_at", StringType()) \
    .add("push_subscribe", StringType()) \
    .add("push_tokens", ArrayType(
    StructType()
        .add("app", StringType())
        .add("device_id", StringType())
        .add("notifications_enabled", BooleanType())
        .add("platform", StringType())
        .add("token", StringType())
)) \
    .add("push_unsubscribed_at", StringType()) \
    .add("random_bucket", LongType()) \
    .add("time_zone", StringType()) \
    .add("total_revenue", DoubleType()) \
    .add("uninstalled_at", StringType()) \
    .add("user_aliases", ArrayType(
    StructType()
        .add("alias_label", StringType())
        .add("alias_name", StringType())
))

stg_user_table='some_stg_tbl'

create_stg_user_tbl_template = """
CREATE EXTERNAL TABLE IF NOT EXISTS  {stg_user_table} (
  `apps` array<struct<first_used:string,last_used:string,name:string,platform:string,sessions:bigint,version:string>>, 
  `system_id` string, 
  `campaigns_received` array<struct<api_campaign_id:string,converted:boolean,engaged:struct<clicked_email:boolean,clicked_triggered_in_app_message:boolean,opened_email:boolean,open_push:boolean>,in_control:boolean,last_received:string,name:string,variation_api_id:string,variation_name:string>>, 
  `canvases_received` array<struct<api_canvas_id:string,in_control:string,is_in_control:boolean,last_entered:string,last_entered_control_at:string,last_exited:string,last_received_message:string,name:string,steps_received:array<struct<api_canvas_step_id:string,last_received:string,name:string>>,variation_name:string>>, 
  `country` string, 
  `created_at` string, 
  `custom_attributes` struct<active_flag:string,date_created:string,email_deliverability_status:string,email_permission_status:string,is_corporate:string,last_email_click_date:string,last_email_delivered_date:string,last_email_open_date:string,last_order_date:string,responsys_last_open_date:string,source_first:string,source_recent:string,zip_code:string>,   
  `custom_events` array<struct<count:bigint,first:string,last:string,name:string>>, 
  `devices` array<struct<ad_tracking_enabled:string,career:string,device_id:string,idfa:string,idfv:string,model:string,os:string>>, 
  `email` string, 
  `email_subscribe` string, 
  `email_unsubscribed_at` string, 
  `external_id` string, 
  `first_name` string, 
  `gender` string, 
  `language` string, 
  `last_name` string, 
  `purchases` array<struct<count:bigint,first:string,last:string,name:string>>, 
  `push_opted_in_at` string, 
  `push_subscribe` string, 
  `push_tokens` array<struct<app:string,device_id:string,notifications_enabled:boolean,platform:string,token:string>>, 
  `push_unsubscribed_at` string, 
  `random_bucket` bigint, 
  `time_zone` string, 
  `total_revenue` double, 
  `uninstalled_at` string, 
  `user_aliases` array<struct<alias_label:string,alias_name:string>>,
  `brand` string
  )
STORED AS PARQUET
LOCATION
'{location}'
TBLPROPERTIES ('parquet.compress'='snappy')
""".format(stg_user_table= stg_user_table, location='{location}')
