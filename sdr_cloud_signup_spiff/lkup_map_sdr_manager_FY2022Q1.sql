CREATE OR REPLACE EXTERNAL TABLE `data-sandbox-123.Workspace_Ashwini.lkup_map_sdr_manager_FY2022Q1`  
(
  sdr_name	STRING,
  manager_name	STRING
)
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['https://docs.google.com/spreadsheets/d/11s_o0ePO-HvJo8DPc5AmGrbGTscrM3A4fo7YVlN0RVo/edit#gid=0'],
  skip_leading_rows = 1
)
