-- transient=true
{{ config(
    materialized='table',
    transient=true,
    pre_hook=[
        "CALL DEV.LH2_EXPLOIT_DEV.SET_GLOBAL_VAR_PROC('S','GL_ACCOUNT_DETAILS_TEMP','LH2_SILVER_DEV.GL_ACCOUNT_DETAILS_TEMP','BEGIN','10')",
        "CALL DEV.LH2_EXPLOIT_DEV.WRITE_LOG_PROC()"
    ],
    post_hook=[
        "CALL DEV.LH2_EXPLOIT_DEV.SET_GLOBAL_VAR_PROC('S','GL_ACCOUNT_DETAILS_TEMP','LH2_SILVER_DEV.GL_ACCOUNT_DETAILS_TEMP','COMPLETED','90',NULL::VARCHAR,NULL::VARCHAR,NULL::TIMESTAMP_NTZ,NULL::TIMESTAMP_NTZ,(SELECT COUNT(*) FROM {{ this }}))",
        "CALL DEV.LH2_EXPLOIT_DEV.WRITE_LOG_PROC()"
    ]
) }}


-- voir pour ajouter exception à
SELECT *
   FROM (
      SELECT
         gcck.CODE_COMBINATION_ID                    CODE_COMBINATION_ID,
         gcck.CHART_OF_ACCOUNTS_ID                   CHART_OF_ACCOUNTS_ID,
         gcck.CONCATENATED_SEGMENTS                  CONCATENATED_SEGMENTS,
         gcck.GL_ACCOUNT_TYPE                        GL_ACCOUNT_TYPE,
         gcck.LAST_UPDATE_DATE                       LAST_UPDATE_DATE   ,
         gcck.LAST_UPDATED_BY                        LAST_UPDATED_BY,
         gcck.ENABLED_FLAG                           ENABLED_FLAG,
         gcck.SUMMARY_FLAG                           SUMMARY_FLAG,
         gcck.SEGMENT1                               BU_GL_SEGMENT1,
         ffvt1.DESCRIPTION                           BU_GL_DESCRIPTION,
         fr1.BU_GL_REGROUPEMENT_ACHAT                BU_GL_REGROUPEMENT_ACHAT,
         fr1.BU_GL_REGROUPEMENT_VENTE                BU_GL_REGROUPEMENT_VENTE,
         gcck.SEGMENT2                               LOCATION_GL_SEGMENT2,
         ffvt2.DESCRIPTION                           LOCATION_GL_DESCRIPTION,
         fr2.LOCATION_GL_REGROUPEMENT_ACHAT          LOCATION_GL_REGROUPEMENT_ACHAT,
         fr2.LOCATION_GL_REGROUPEMENT_VENTE          LOCATION_GL_REGROUPEMENT_VENTE,
         fr2.LOCATION_GL_BU_VENTE                    LOCATION_GL_BU_VENTE,
         fr2.CLE_BU_INVOICE                          CLE_BU_INVOICE,
         gcck.SEGMENT3                               DEPARTMENT_GL_SEGMENT3,
         ffvt3.DESCRIPTION                           DEPARTMENT_GL_DESCRIPTION,
         fr3.DEPARTMENT_GL_REGROUPEMENT_ACHAT        DEPARTMENT_GL_REGROUPEMENT_ACHAT,
         fr3.DEPARTMENT_GL_REGROUPEMENT_VENTE        DEPARTMENT_GL_REGROUPEMENT_VENTE,
         fr3.DEPARTMENT_GL_BU_VENTE                  DEPARTMENT_GL_BU_VENTE,
         gcck.SEGMENT4                               NATURAL_ACCOUNT_GL_SEGMENT4,
         ffvt4.DESCRIPTION                           NATURAL_ACCOUNT_GL_DESCRIPTION,
         fr4.flag_item_ls_sales                      FLAG_ITEM_LS_SALES,
         fr4.NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_1       NATURAL_ACCOUNT_GL_REGROUPEMENT_1,
         fr4.NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_2       NATURAL_ACCOUNT_GL_REGROUPEMENT_2,
         fr4.NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_3       NATURAL_ACCOUNT_GL_REGROUPEMENT_3,
         gcck.SEGMENT5                               PRODUCT_GROUP_GL_SEGMENT5,
         ffvt5.DESCRIPTION                           PRODUCT_GROUP_GL_DESCRIPTION,
         fr5.PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT     PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT,
         fr5.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE     PRODUCT_GROUP_GL_REGROUPEMENT_VENTE,
         fr5.PRODUCT_GROUP_GL_BU_VENTE               PRODUCT_GROUP_GL_BU_VENTE,
         gcck.SEGMENT6                               INTERCOMPANY_GL_SEGMENT6,
         ffvt6.DESCRIPTION                           INTERCOMPANY_GL_DESCRIPTION,
         fr6.INTERCOMPANY_GL_REGROUPEMENT_ACHAT      INTERCOMPANY_GL_REGROUPEMENT_ACHAT,
         fr6.INTERCOMPANY_TYPE                       INTERCOMPANY_TYPE,
         fr6.PERIMETRE_LS_CONSO                      PERIMETRE_LS_CONSO,
         ROWNUM                                      ROW_NUMBER_ID,
         SYSDATE                                     ROW_CREATION_DATE,
         SYSDATE                                     ROW_LAST_UPDATE_DATE
      FROM DEV.LH2_BRONZE_DEV.STEPH_APPS_GL_CODE_COMBINATIONS_KFV        gcck
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.file_bu_segment1                fr1
         ON gcck.segment1 = fr1.bu_gl_code
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES     ffv1
         ON gcck.SEGMENT1 = ffv1.flex_value
         AND ffv1.flex_value_set_id = 1014124
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES_tl  ffvt1
         ON ffv1.flex_value_id = ffvt1.flex_value_id
         AND ffvt1.language = 'US'
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.file_location_segment2          fr2
         ON gcck.segment2 = fr2.location_gl_code
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES     ffv2
         ON gcck.SEGMENT2 = ffv2.flex_value
         AND ffv2.flex_value_set_id = 1014125
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES_tl   ffvt2
         ON ffv2.flex_value_id = ffvt2.flex_value_id
         AND ffvt2.language = 'US'
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.file_department_segment3        fr3
         ON gcck.segment3 = fr3.department_gl_code
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES     ffv3
         ON gcck.SEGMENT3 = ffv3.flex_value
         AND ffv3.flex_value_set_id = 1014126
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES_tl   ffvt3
         ON ffv3.flex_value_id = ffvt3.flex_value_id
         AND ffvt3.language = 'US'
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.file_naturalaccount_segment4    fr4
         ON gcck.segment4 = fr4.natural_account_gl_code
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES     ffv4
         ON gcck.SEGMENT4 = ffv4.flex_value
         AND ffv4.flex_value_set_id = 1014127
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES_tl   ffvt4
         ON ffv4.flex_value_id = ffvt4.flex_value_id
         AND ffvt4.language = 'US'
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.file_productgroup_segment5      fr5
         ON gcck.segment5 = fr5.product_group_gl_code
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES     ffv5
         ON gcck.SEGMENT5 = ffv5.flex_value
         AND ffv5.flex_value_set_id = 1014129
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES_tl   ffvt5
         ON ffv5.flex_value_id = ffvt5.flex_value_id
         AND ffvt5.language = 'US'
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.file_interco_segment6           fr6
         ON gcck.segment6 = fr6.intercompany_gl_code
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES     ffv6
         ON gcck.SEGMENT6 = ffv6.flex_value
         AND ffv6.flex_value_set_id = 1014128
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES_tl   ffvt6
         ON ffv6.flex_value_id = ffvt6.flex_value_id
         AND ffvt6.language = 'US'
