-- transient=true
{{ config(
    materialized='table',
    transient=true,
    pre_hook=[
        "CALL DEV.LH2_EXPLOIT_DEV.SET_GLOBAL_VAR_PROC('S','GL_SEGMENT2_LOCATION_TEMP','LH2_SILVER_DEV.GL_SEGMENT2_LOCATION_TEMP','BEGIN','10')",
        "CALL DEV.LH2_EXPLOIT_DEV.WRITE_LOG_PROC()"
    ],
    post_hook=[
        "CALL DEV.LH2_EXPLOIT_DEV.SET_GLOBAL_VAR_PROC('S','GL_SEGMENT2_LOCATION_TEMP','LH2_SILVER_DEV.GL_SEGMENT2_LOCATION_TEMP','COMPLETED','90',NULL::VARCHAR,NULL::VARCHAR,NULL::TIMESTAMP_NTZ,NULL::TIMESTAMP_NTZ,(SELECT COUNT(*) FROM {{ this }}))",
        "CALL DEV.LH2_EXPLOIT_DEV.WRITE_LOG_PROC()"
    ]
) }}


-- voir pour ajouter exception à
SELECT *
   FROM (
      SELECT
         ffv.flex_value                                        LOCATION_GL_SEGMENT2,
         fifs.id_flex_num                                      CHART_OF_ACCOUNTS_ID,
         ffvt.DESCRIPTION                                      LOCATION_GL_DESCRIPTION,
         nvl(fr2.LOCATION_GL_REGROUPEMENT_VENTE,'UNDEFINED')   LOCATION_GL_REGROUPEMENT_VENTE,
         nvl(fr2.LOCATION_GL_REGROUPEMENT_ACHAT,'UNDEFINED')   LOCATION_GL_REGROUPEMENT_ACHAT,
         nvl(fr2.LOCATION_GL_BU_VENTE,'NOT AFFECTED')          LOCATION_GL_BU_VENTE,
         nvl(fr2.CLE_BU_INVOICE,'segment_5')                   CLE_BU_INVOICE,
         fr2.DATE_MAJ                                          DATE_MAJ_EXCEL,
         fr2.COMMENTAIRES                                      COMMENTAIRES,
         ffv.CREATION_DATE                                     CREATION_DATE   ,
         ffv.CREATED_BY                                        CREATED_BY,
         ffv.LAST_UPDATE_DATE                                  LAST_UPDATE_DATE   ,
         ffv.LAST_UPDATED_BY                                   LAST_UPDATED_BY,
         ROWNUM                                                ROW_NUMBER_ID,
         SYSDATE                                               ROW_CREATION_DATE,
         SYSDATE                                               ROW_LAST_UPDATE_DATE
      FROM DEV.LH2_BRONZE_DEV.steph_apps_FND_ID_FLEX_SEGMENTS_VL fifs
      INNER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES ffv
         ON ( fifs.flex_value_set_id = ffv.flex_value_set_id )
      INNER JOIN DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES_tl ffvt
         ON ( ffv.flex_value_id = ffvt.flex_value_id
         AND ffvt.language = 'US' )
      LEFT OUTER JOIN DEV.LH2_BRONZE_DEV.file_location_segment2 fr2
         ON ffv.flex_value  = fr2.location_gl_code
      WHERE
         fifs.id_flex_num = 101
         AND fifs.id_flex_code = 'GL#'
         AND fifs.segment_num  = 2
