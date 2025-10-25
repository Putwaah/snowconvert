-- transient=true
{{ config(
    materialized='table',
    transient=true,
    pre_hook=[
        "CALL DEV.LH2_EXPLOIT_DEV.SET_GLOBAL_VAR_PROC('S','INTERCOMPANY_PARAMETERS_TEMP','LH2_SILVER_DEV.INTERCOMPANY_PARAMETERS_TEMP','BEGIN','10')",
        "CALL DEV.LH2_EXPLOIT_DEV.WRITE_LOG_PROC()"
    ],
    post_hook=[
        "CALL DEV.LH2_EXPLOIT_DEV.SET_GLOBAL_VAR_PROC('S','INTERCOMPANY_PARAMETERS_TEMP','LH2_SILVER_DEV.INTERCOMPANY_PARAMETERS_TEMP','COMPLETED','90',NULL::VARCHAR,NULL::VARCHAR,NULL::TIMESTAMP_NTZ,NULL::TIMESTAMP_NTZ,(SELECT COUNT(*) FROM {{ this }}))",
        "CALL DEV.LH2_EXPLOIT_DEV.WRITE_LOG_PROC()"
    ]
) }}


-- voir pour ajouter exception à
SELECT
      t.* ,
      rownum                      ROW_NUMBER_ID,
      sysdate                     ROW_CREATION_DATE ,
      sysdate                     ROW_LAST_UPDATE_DATE
   FROM DEV.LH2_BRONZE_DEV.STEPH_APPS_MTL_INTERCOMPANY_PARAMETERS t
