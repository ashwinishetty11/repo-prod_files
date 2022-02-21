CREATE OR REPLACE EXTERNAL TABLE sdr_cloud_signup_spiff.lkup_sdr_manager 
(
  sdr_id	    STRING,
  sdr_name	    STRING,
  manager_id	STRING,
  manager_name	STRING
)
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['https://docs.google.com/spreadsheets/d/1c8pyE23Rcw3WBDlWMQ9OdEZt05d4iaW0-LN6sRLfMCo/edit#gid=0'],
  sheet_range = 'lkup_sdr_manager',
  skip_leading_rows = 1
);

create or replace table sdr_cloud_signup_spiff.lkup_sdr_manager_stg as
(
    select
        sdr_id,
        sdr_name,
        manager_id,
        manager_name,
        CURRENT_TIMESTAMP() AS _snapshot_ts
    from sdr_cloud_signup_spiff.lkup_sdr_manager
); 
