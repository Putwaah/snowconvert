create or replace PACKAGE BODY LH2_DTH_SILVER_CORE_PKG IS

/*     $Header: LH2_DTH_SILVER_CORE_PKG.sql 1.0.0 2024/06/11 09:00:00 vsre noship $ */
-- ***************************************************************************
-- @(#) ----------------------------------------------------------------------
-- @(#) Specifique: DTH005 IMPORT des données dans le Silver
-- @(#) Fichier   : .LH2_DTH_SILVER_CORE_PKG.sql
-- @(#) Version   : 1.0.0 du 11/06/2024
-- @(#) ---------------------------------------------e-------------------------
-- Objet          : Package Body du DTH005 IMPORT des données Silver
-- Commentaires   :
-- Exemple        :
-- ***************************************************************************
--                            HISTORIQUE DES VERSIONS
-- ---------------------------------------------------------------------------
-- Date     Version  Nom            Description de l'intervention
-- -------- -------- -------------- ------------------------------------------
-- 11/06/24  1.0.0   JABIT          Version initiale
-- 31/07/24  1.0.1   POTC           Ajout création en dur des IO et OU pour la Corée et Singapour
-- 09/08/24  1.0.2   POTC           Ajout de la création de la table STRUCTURE_RESEAU
-- 12/09/24  1.0.3   POTC           Réorganisation de la gestion des logs
-- 04/11/24  2.0.0   POTC           Changement de la gestion des logs
-- 15/11/24  2.0.1   POTC           Ajout créattion INTERCOMPANY_PARAMETERS_TEMP
-- 20/11/24  2.0.2   POTC           Ajout creation GL_SEGMENTx_xxx_TEMP lien entre segement oracle et excel de regroupement
-- 06/12/24  2.0.3   BIHAN          Ajout des packages Daily_conversion_rates & Fixed_rate
-- 03/01/25  2.0.4   POTC           Modification de la création de la table GL_SEGMENT4_NATURAL_ACCOUNT_TEMP
-- 07/01/25  2.0.5   GOICHON        Ajout procédure création COUNTRY_ZONE
-- 23/01/25  2.0.6   POTC           Ajout précoédure création Var_Customer_Segement_Bu
-- 27/01/25  2.0.7   GOICHON        Ajout procédure création commande_hors_backlog
-- 28/02/25  2.0.8   BIHAN          Ajout création table ORDER_CHARGES
-- 03/03/25  2.0.9   COUVYS         Ajout appel procedure LH2_EXPLOIT_PKG.LH2_POST_BRONZE_PROC pou update synonyms
-- 14/05/25  3.0.0   GOICHONA       Ajout de DOCUMENT_EIA_LINE_TEXT_TEMP
-- 16/06/25  3.0.1   POTC           Ajout d'un INSERT pour rajouter les natural account pour les pays not oracle
-- 26/06/25  3.0.2   POTC           Ajout création table MANUAL_ADJUSTMENTS
-- 04/07/25  3.0.3   POTC           Ajout de la création de l'OU et IO de l'AUSTRALIA
-- 12/08/25  3.0.4   GOICHON        Ajout de la CURRENCY a COUNTRY_ZONE
-- 17/09/25  3.0.5   GOICHON        Ajout procédure pour créer steph_apps_fnd_flex_value_tl afin de voir si cela améliorer les performance du gold lors de jointure sur cette table
-- 25/09/25  3.0.6   OJABIT         Ajout procédure pour créer FND_FLEX_VALUES_US qui regroupe les jointures FND_FLEX_VALUES
-- 29/09/25  3.0.7   OJABIT         Modification du champ INVOICED_AMOUNT_IN_TRANSACTIONAL_CURRENCY en INVOICED_AMOUNT_IN_USD_FIXED
-- 21/10/25  3.0.8   DEROUBAIX      Ajout de la table FILE_VAR_BU FILE_VAR_SUBBU

-- ***************************************************************************

/****************************************************************************************
* PROCEDURE   :  Exceptions_PROC
* DESCRIPTION :  Procedure générique pour les exceptions
*
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Exceptions_PROC
IS
BEGIN
   g_erreur_pkg := 1 ;

   LH2_EXPLOIT_PKG.SET_GLOBAL_VAR_PROC(
      g_programme,
      g_etape,
      g_level,
      g_table,
      g_status,
      g_error_code,
      g_error_msg,
      g_date_deb,
      g_date_fin,
      g_rowcount
   );

   LH2_EXPLOIT_PKG.EXCEPTIONS_PROC;

END Exceptions_PROC;

/****************************************************************************************
* PROCEDURE   :  Write_Log_PROC
* DESCRIPTION :  Procedure générique pour la génération des Logs
*
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------*/

PROCEDURE Write_Log_PROC
IS
BEGIN  --Début traitement

--  g_status    :='COMPLETED';
   g_date_fin  :=sysdate;
   g_rowcount := SQL%ROWCOUNT;

   LH2_EXPLOIT_PKG.SET_GLOBAL_VAR_PROC(
      g_programme,
      g_etape,
      g_level,
      g_table,
      g_status,
      g_error_code,
      g_error_msg,
      g_date_deb,
      g_date_fin,
      g_rowcount
   );

   LH2_EXPLOIT_PKG.WRITE_LOG_PROC;

   -- g_date_deb  :=sysdate;

END Write_Log_PROC;

/****************************************************************************************
* PROCEDURE   :  Recreate_Gl_Account_Details_Proc
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Gl_Account_Details_Proc
IS
   v_procedure varchar2(100) := 'Recreate_Gl_Account_Details_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'GL_ACCOUNT_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE GL_ACCOUNT_DETAILS_TEMP'  ;
   g_status    := 'COMPLETED';
   g_etape     := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'GL_ACCOUNT_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;

   INSERT INTO GL_ACCOUNT_DETAILS_TEMP
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
      FROM STEPH_APPS_GL_CODE_COMBINATIONS_KFV_bz        gcck
      LEFT OUTER JOIN file_bu_segment1_bz                fr1
         ON gcck.segment1 = fr1.bu_gl_code
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES#_bz     ffv1
         ON gcck.SEGMENT1 = ffv1.flex_value
         AND ffv1.flex_value_set_id = 1014124
      LEFT OUTER JOIN  steph_apps_FND_FLEX_VALUES_tl_bz  ffvt1
         ON ffv1.flex_value_id = ffvt1.flex_value_id
         AND ffvt1.language = 'US'
      LEFT OUTER JOIN file_location_segment2_bz          fr2
         ON gcck.segment2 = fr2.location_gl_code
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES#_bz     ffv2
         ON gcck.SEGMENT2 = ffv2.flex_value
         AND ffv2.flex_value_set_id = 1014125
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES_tl_bz   ffvt2
         ON ffv2.flex_value_id = ffvt2.flex_value_id
         AND ffvt2.language = 'US'
      LEFT OUTER JOIN file_department_segment3_bz        fr3
         ON gcck.segment3 = fr3.department_gl_code
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES#_bz     ffv3
         ON gcck.SEGMENT3 = ffv3.flex_value
         AND ffv3.flex_value_set_id = 1014126
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES_tl_bz   ffvt3
         ON ffv3.flex_value_id = ffvt3.flex_value_id
         AND ffvt3.language = 'US'
      LEFT OUTER JOIN file_naturalaccount_segment4_bz    fr4
         ON gcck.segment4 = fr4.natural_account_gl_code
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES#_bz     ffv4
         ON gcck.SEGMENT4 = ffv4.flex_value
         AND ffv4.flex_value_set_id = 1014127
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES_tl_bz   ffvt4
         ON ffv4.flex_value_id = ffvt4.flex_value_id
         AND ffvt4.language = 'US'
      LEFT OUTER JOIN file_productgroup_segment5_bz      fr5
         ON gcck.segment5 = fr5.product_group_gl_code
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES#_bz     ffv5
         ON gcck.SEGMENT5 = ffv5.flex_value
         AND ffv5.flex_value_set_id = 1014129
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES_tl_bz   ffvt5
         ON ffv5.flex_value_id = ffvt5.flex_value_id
         AND ffvt5.language = 'US'
      LEFT OUTER JOIN file_interco_segment6_bz           fr6
         ON gcck.segment6 = fr6.intercompany_gl_code
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES#_bz     ffv6
         ON gcck.SEGMENT6 = ffv6.flex_value
         AND ffv6.flex_value_set_id = 1014128
      LEFT OUTER JOIN steph_apps_FND_FLEX_VALUES_tl_bz   ffvt6
         ON ffv6.flex_value_id = ffvt6.flex_value_id
         AND ffvt6.language = 'US'
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_ACCOUNT_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('GL_ACCOUNT_DETAILS_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'   ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_Gl_Account_Details_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_IO_Details_Proc
* DESCRIPTION :  création table regroupant les informations liées à l'IO dans différentes tables oracle dans une seule table
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_IO_Details_Proc
IS
   v_procedure varchar2(100) := 'Recreate_IO_Details_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'IO_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
         EXECUTE IMMEDIATE 'TRUNCATE TABLE IO_DETAILS_TEMP';
   g_status    := 'COMPLETED';
   g_etape     := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'IO_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO IO_DETAILS_TEMP (
      SELECT
         OOD.ORGANIZATION_ID                 OOD_ORGANIZATION_ID,
         OOD.BUSINESS_GROUP_ID               OOD_BUSINESS_GROUP_ID,
         OOD.USER_DEFINITION_ENABLE_DATE     OOD_USER_DEFINITION_ENABLE_DATE,
         OOD.DISABLE_DATE                    OOD_DISABLE_DATE,
         OOD.ORGANIZATION_CODE               OOD_ORGANIZATION_CODE,
         OOD.ORGANIZATION_NAME               OOD_ORGANIZATION_NAME,
         OOD.SET_OF_BOOKS_ID                 OOD_SET_OF_BOOKS_ID,
         OOD.CHART_OF_ACCOUNTS_ID            OOD_CHART_OF_ACCOUNTS_ID,
         OOD.INVENTORY_ENABLED_FLAG          OOD_INVENTORY_ENABLED_FLAG,
         OOD.OPERATING_UNIT                  OOD_OPERATING_UNIT,
         OOD.LEGAL_ENTITY                    OOD_LEGAL_ENTITY,
         HAOU.ORGANIZATION_ID                HAOU_ORGANIZATION_ID,
         HAOU.BUSINESS_GROUP_ID              HAOU_BUSINESS_GROUP_ID,
         HAOU.COST_ALLOCATION_KEYFLEX_ID     HAOU_COST_ALLOCATION_KEYFLEX_ID,
         HAOU.LOCATION_ID                    HAOU_LOCATION_ID,
         HAOU.SOFT_CODING_KEYFLEX_ID         HAOU_SOFT_CODING_KEYFLEX_ID,
         HAOU.DATE_FROM                      HAOU_DATE_FROM,
         HAOU.NAME                           HAOU_NAME,
         HAOU.DATE_TO                        HAOU_DATE_TO,
         HAOU.INTERNAL_EXTERNAL_FLAG         HAOU_INTERNAL_EXTERNAL_FLAG,
         HAOU.INTERNAL_ADDRESS_LINE          HAOU_INTERNAL_ADDRESS_LINE,
         HAOU.TYPE                           HAOU_TYPE,
         HAOU.REQUEST_ID                     HAOU_REQUEST_ID,
         HAOU.PROGRAM_APPLICATION_ID         HAOU_PROGRAM_APPLICATION_ID,
         HAOU.PROGRAM_ID                     HAOU_PROGRAM_ID,
         HAOU.PROGRAM_UPDATE_DATE            HAOU_PROGRAM_UPDATE_DATE,
         HAOU.ATTRIBUTE_CATEGORY             HAOU_ATTRIBUTE_CATEGORY,
         HAOU.ATTRIBUTE1                     HAOU_ATTRIBUTE1,
         HAOU.ATTRIBUTE2                     HAOU_ATTRIBUTE2,
         HAOU.ATTRIBUTE3                     HAOU_ATTRIBUTE3,
         HAOU.ATTRIBUTE4                     HAOU_ATTRIBUTE4,
         HAOU.ATTRIBUTE5                     HAOU_ATTRIBUTE5,
         HAOU.ATTRIBUTE6                     HAOU_ATTRIBUTE6,
         HAOU.ATTRIBUTE7                     HAOU_ATTRIBUTE7,
         HAOU.ATTRIBUTE8                     HAOU_ATTRIBUTE8,
         HAOU.ATTRIBUTE9                     HAOU_ATTRIBUTE9,
         HAOU.ATTRIBUTE10                    HAOU_ATTRIBUTE10,
         HAOU.ATTRIBUTE11                    HAOU_ATTRIBUTE11,
         HAOU.ATTRIBUTE12                    HAOU_ATTRIBUTE12,
         HAOU.ATTRIBUTE13                    HAOU_ATTRIBUTE13,
         HAOU.ATTRIBUTE14                    HAOU_ATTRIBUTE14,
         HAOU.ATTRIBUTE15                    HAOU_ATTRIBUTE15,
         HAOU.ATTRIBUTE16                    HAOU_ATTRIBUTE16,
         HAOU.ATTRIBUTE17                    HAOU_ATTRIBUTE17,
         HAOU.ATTRIBUTE18                    HAOU_ATTRIBUTE18,
         HAOU.ATTRIBUTE19                    HAOU_ATTRIBUTE19,
         HAOU.ATTRIBUTE20                    HAOU_ATTRIBUTE20,
         HAOU.LAST_UPDATE_DATE               HAOU_LAST_UPDATE_DATE,
         HAOU.LAST_UPDATED_BY                HAOU_LAST_UPDATED_BY,
         HAOU.LAST_UPDATE_LOGIN              HAOU_LAST_UPDATE_LOGIN,
         HAOU.CREATED_BY                     HAOU_CREATED_BY,
         HAOU.CREATION_DATE                  HAOU_CREATION_DATE,
         HAOU.OBJECT_VERSION_NUMBER          HAOU_OBJECT_VERSION_NUMBER,
         HAOU.PARTY_ID                       HAOU_PARTY_ID,
         HAOU.COMMENTS                       HAOU_COMMENTS,
         HAOU.ATTRIBUTE21                    HAOU_ATTRIBUTE21,
         HAOU.ATTRIBUTE22                    HAOU_ATTRIBUTE22,
         HAOU.ATTRIBUTE23                    HAOU_ATTRIBUTE23,
         HAOU.ATTRIBUTE24                    HAOU_ATTRIBUTE24,
         HAOU.ATTRIBUTE25                    HAOU_ATTRIBUTE25,
         HAOU.ATTRIBUTE26                    HAOU_ATTRIBUTE26,
         HAOU.ATTRIBUTE27                    HAOU_ATTRIBUTE27,
         HAOU.ATTRIBUTE28                    HAOU_ATTRIBUTE28,
         HAOU.ATTRIBUTE29                    HAOU_ATTRIBUTE29,
         HAOU.ATTRIBUTE30                    HAOU_ATTRIBUTE30,
         HOU.BUSINESS_GROUP_ID               HOU_BUSINESS_GROUP_ID,
         HOU.ORGANIZATION_ID                 HOU_ORGANIZATION_ID,
         HOU.NAME                            HOU_NAME,
         HOU.DATE_FROM                       HOU_DATE_FROM,
         HOU.DATE_TO                         HOU_DATE_TO,
         HOU.SHORT_CODE                      HOU_SHORT_CODE,
         HOU.SET_OF_BOOKS_ID                 HOU_SET_OF_BOOKS_ID,
         HOU.DEFAULT_LEGAL_CONTEXT_ID        HOU_DEFAULT_LEGAL_CONTEXT_ID,
         HOU.USABLE_FLAG                     HOU_USABLE_FLAG,
         HLA.LOCATION_ID                     HLA_LOCATION_ID,
         HLA.LOCATION_CODE                   HLA_LOCATION_CODE,
         HLA.BUSINESS_GROUP_ID               HLA_BUSINESS_GROUP_ID,
         HLA.DESCRIPTION                     HLA_DESCRIPTION,
         HLA.SHIP_TO_LOCATION_ID             HLA_SHIP_TO_LOCATION_ID,
         HLA.SHIP_TO_SITE_FLAG               HLA_SHIP_TO_SITE_FLAG,
         HLA.RECEIVING_SITE_FLAG             HLA_RECEIVING_SITE_FLAG,
         HLA.BILL_TO_SITE_FLAG               HLA_BILL_TO_SITE_FLAG,
         HLA.IN_ORGANIZATION_FLAG            HLA_IN_ORGANIZATION_FLAG,
         HLA.OFFICE_SITE_FLAG                HLA_OFFICE_SITE_FLAG,
         HLA.DESIGNATED_RECEIVER_ID          HLA_DESIGNATED_RECEIVER_ID,
         HLA.INVENTORY_ORGANIZATION_ID       HLA_INVENTORY_ORGANIZATION_ID,
         HLA.TAX_NAME                        HLA_TAX_NAME,
         HLA.INACTIVE_DATE                   HLA_INACTIVE_DATE,
         HLA.STYLE                           HLA_STYLE,
         HLA.ADDRESS_LINE_1                  HLA_ADDRESS_LINE_1,
         HLA.ADDRESS_LINE_2                  HLA_ADDRESS_LINE_2,
         HLA.ADDRESS_LINE_3                  HLA_ADDRESS_LINE_3,
         HLA.TOWN_OR_CITY                    HLA_TOWN_OR_CITY,
         HLA.COUNTRY                         HLA_COUNTRY,
         HLA.POSTAL_CODE                     HLA_POSTAL_CODE,
         HLA.REGION_1                        HLA_REGION_1,
         HLA.REGION_2                        HLA_REGION_2,
         HLA.REGION_3                        HLA_REGION_3,
         HLA.TELEPHONE_NUMBER_1              HLA_TELEPHONE_NUMBER_1,
         HLA.TELEPHONE_NUMBER_2              HLA_TELEPHONE_NUMBER_2,
         HLA.TELEPHONE_NUMBER_3              HLA_TELEPHONE_NUMBER_3,
         HLA.LOC_INFORMATION13               HLA_LOC_INFORMATION13,
         HLA.LOC_INFORMATION14               HLA_LOC_INFORMATION14,
         HLA.LOC_INFORMATION15               HLA_LOC_INFORMATION15,
         HLA.LOC_INFORMATION16               HLA_LOC_INFORMATION16,
         HLA.LOC_INFORMATION17               HLA_LOC_INFORMATION17,
         HLA.ATTRIBUTE_CATEGORY              HLA_ATTRIBUTE_CATEGORY,
         HLA.ATTRIBUTE1                      HLA_ATTRIBUTE1,
         HLA.ATTRIBUTE2                      HLA_ATTRIBUTE2,
         HLA.ATTRIBUTE3                      HLA_ATTRIBUTE3,
         HLA.ATTRIBUTE4                      HLA_ATTRIBUTE4,
         HLA.ATTRIBUTE5                      HLA_ATTRIBUTE5,
         HLA.ATTRIBUTE6                      HLA_ATTRIBUTE6,
         HLA.ATTRIBUTE7                      HLA_ATTRIBUTE7,
         HLA.ATTRIBUTE8                      HLA_ATTRIBUTE8,
         HLA.ATTRIBUTE9                      HLA_ATTRIBUTE9,
         HLA.ATTRIBUTE10                     HLA_ATTRIBUTE10,
         HLA.ATTRIBUTE11                     HLA_ATTRIBUTE11,
         HLA.ATTRIBUTE12                     HLA_ATTRIBUTE12,
         HLA.ATTRIBUTE13                     HLA_ATTRIBUTE13,
         HLA.ATTRIBUTE14                     HLA_ATTRIBUTE14,
         HLA.ATTRIBUTE15                     HLA_ATTRIBUTE15,
         HLA.ATTRIBUTE16                     HLA_ATTRIBUTE16,
         HLA.ATTRIBUTE17                     HLA_ATTRIBUTE17,
         HLA.ATTRIBUTE18                     HLA_ATTRIBUTE18,
         HLA.ATTRIBUTE19                     HLA_ATTRIBUTE19,
         HLA.ATTRIBUTE20                     HLA_ATTRIBUTE20,
         HLA.GLOBAL_ATTRIBUTE_CATEGORY       HLA_GLOBAL_ATTRIBUTE_CATEGORY,
         HLA.GLOBAL_ATTRIBUTE1               HLA_GLOBAL_ATTRIBUTE1,
         HLA.GLOBAL_ATTRIBUTE2               HLA_GLOBAL_ATTRIBUTE2,
         HLA.GLOBAL_ATTRIBUTE3               HLA_GLOBAL_ATTRIBUTE3,
         HLA.GLOBAL_ATTRIBUTE4               HLA_GLOBAL_ATTRIBUTE4,
         HLA.GLOBAL_ATTRIBUTE5               HLA_GLOBAL_ATTRIBUTE5,
         HLA.GLOBAL_ATTRIBUTE6               HLA_GLOBAL_ATTRIBUTE6,
         HLA.GLOBAL_ATTRIBUTE7               HLA_GLOBAL_ATTRIBUTE7,
         HLA.GLOBAL_ATTRIBUTE8               HLA_GLOBAL_ATTRIBUTE8,
         HLA.GLOBAL_ATTRIBUTE9               HLA_GLOBAL_ATTRIBUTE9,
         HLA.GLOBAL_ATTRIBUTE10              HLA_GLOBAL_ATTRIBUTE10,
         HLA.GLOBAL_ATTRIBUTE11              HLA_GLOBAL_ATTRIBUTE11,
         HLA.GLOBAL_ATTRIBUTE12              HLA_GLOBAL_ATTRIBUTE12,
         HLA.GLOBAL_ATTRIBUTE13              HLA_GLOBAL_ATTRIBUTE13,
         HLA.GLOBAL_ATTRIBUTE14              HLA_GLOBAL_ATTRIBUTE14,
         HLA.GLOBAL_ATTRIBUTE15              HLA_GLOBAL_ATTRIBUTE15,
         HLA.GLOBAL_ATTRIBUTE16              HLA_GLOBAL_ATTRIBUTE16,
         HLA.GLOBAL_ATTRIBUTE17              HLA_GLOBAL_ATTRIBUTE17,
         HLA.GLOBAL_ATTRIBUTE18              HLA_GLOBAL_ATTRIBUTE18,
         HLA.GLOBAL_ATTRIBUTE19              HLA_GLOBAL_ATTRIBUTE19,
         HLA.GLOBAL_ATTRIBUTE20              HLA_GLOBAL_ATTRIBUTE20,
         HLA.LAST_UPDATE_DATE                HLA_LAST_UPDATE_DATE,
         HLA.LAST_UPDATED_BY                 HLA_LAST_UPDATED_BY,
         HLA.LAST_UPDATE_LOGIN               HLA_LAST_UPDATE_LOGIN,
         HLA.CREATED_BY                      HLA_CREATED_BY,
         HLA.CREATION_DATE                   HLA_CREATION_DATE,
         HLA.ENTERED_BY                      HLA_ENTERED_BY,
         HLA.TP_HEADER_ID                    HLA_TP_HEADER_ID,
         HLA.ECE_TP_LOCATION_CODE            HLA_ECE_TP_LOCATION_CODE,
         HLA.OBJECT_VERSION_NUMBER           HLA_OBJECT_VERSION_NUMBER,
         HLA.GEOMETRY                        HLA_GOEMETRY,
         HLA.LOC_INFORMATION18               HLA_LOC_INFORMATION18,
         HLA.LOC_INFORMATION19               HLA_LOC_INFORMATION19,
         HLA.LOC_INFORMATION20               HLA_LOC_INFORMATION20,
         HLA.DERIVED_LOCALE                  HLA_DERIVED_LOCALE ,
         HLA.LEGAL_ADDRESS_FLAG              HLA_LEGAL_ADDRESS_FLAG,
         HLA.TIMEZONE_CODE                   HLA_TIMEZONE_CODE,
         MP.ORGANIZATION_ID                  MP_ORGANIZATION_ID,
         MP.LAST_UPDATE_DATE                 MP_LAST_UPDATE_DATE,
         MP.LAST_UPDATED_BY                  MP_LAST_UPDATED_BY,
         MP.CREATION_DATE                    MP_CREATION_DATE,
         MP.CREATED_BY                       MP_CREATED_BY,
         MP.LAST_UPDATE_LOGIN                MP_LAST_UPDATE_LOGIN,
         MP.ORGANIZATION_CODE                MP_ORGANIZATION_CODE,
         MP.MASTER_ORGANIZATION_ID           MP_MASTER_ORGANIZATION_ID,
         MP.PRIMARY_COST_METHOD              MP_PRIMARY_COST_METHOD,
         MP.COST_ORGANIZATION_ID             MP_COST_ORGANIZATION_ID,
         MP.DEFAULT_MATERIAL_COST_ID         MP_DEFAULT_MATERIAL_COST_ID,
         MP.CALENDAR_EXCEPTION_SET_ID        MP_CALENDAR_EXCEPTION_SET_ID,
         MP.CALENDAR_CODE                    MP_CALENDAR_CODE,
         MP.GENERAL_LEDGER_UPDATE_CODE       MP_GENERAL_LEDGER_UPDATE_CODE,
         MP.DEFAULT_ATP_RULE_ID              MP_DEFAULT_ATP_RULE_ID,
         MP.DEFAULT_PICKING_RULE_ID          MP_DEFAULT_PICKING_RULE_ID,
         MP.DEFAULT_LOCATOR_ORDER_VALUE      MP_DEFAULT_LOCATOR_ORDER_VALUE,
         MP.DEFAULT_SUBINV_ORDER_VALUE       MP_DEFAULT_SUBINV_ORDER_VALUE,
         MP.NEGATIVE_INV_RECEIPT_CODE        MP_NEGATIVE_INV_RECEIPT_CODE,
         MP.STOCK_LOCATOR_CONTROL_CODE       MP_STOCK_LOCATOR_CONTROL_CODE,
         MP.MATERIAL_ACCOUNT                 MP_MATERIAL_ACCOUNT,
         MP.MATERIAL_OVERHEAD_ACCOUNT        MP_MATERIAL_OVERHEAD_ACCOUNT,
         MP.MATL_OVHD_ABSORPTION_ACCT        MP_MATL_OVHD_ABSORPTION_ACCT,
         MP.RESOURCE_ACCOUNT                 MP_RESOURCE_ACCOUNT,
         MP.PURCHASE_PRICE_VAR_ACCOUNT       MP_PURCHASE_PRICE_VAR_ACCOUNT,
         MP.AP_ACCRUAL_ACCOUNT               MP_AP_ACCRUAL_ACCOUNT,
         MP.OVERHEAD_ACCOUNT                 MP_OVERHEAD_ACCOUNT,
         MP.OUTSIDE_PROCESSING_ACCOUNT       MP_OUTSIDE_PROCESSING_ACCOUNT,
         MP.INTRANSIT_INV_ACCOUNT            MP_INTRANSIT_INV_ACCOUNT,
         MP.INTERORG_RECEIVABLES_ACCOUNT     MP_INTERORG_RECEIVABLES_ACCOUNT,
         MP.INTERORG_PRICE_VAR_ACCOUNT       MP_INTERORG_PRICE_VAR_ACCOUNT,
         MP.INTERORG_PAYABLES_ACCOUNT        MP_INTERORG_PAYABLES_ACCOUNT,
         MP.COST_OF_SALES_ACCOUNT            MP_COST_OF_SALES_ACCOUNT,
         MP.ENCUMBRANCE_ACCOUNT              MP_ENCUMBRANCE_ACCOUNT,
         MP.PROJECT_COST_ACCOUNT             MP_PROJECT_COST_ACCOUNT,
         MP.INTERORG_TRANSFER_CR_ACCOUNT     MP_INTERORG_TRANSFER_CR_ACCOUNT,
         MP.MATL_INTERORG_TRANSFER_CODE      MP_MATL_INTERORG_TRANSFER_CODE,
         MP.INTERORG_TRNSFR_CHARGE_PERCENT   MP_INTERORG_TRNSFR_CHARGE_PERCENT,
         MP.SOURCE_ORGANIZATION_ID           MP_SOURCE_ORGANIZATION_ID,
         MP.SOURCE_SUBINVENTORY              MP_SOURCE_SUBINVENTORY,
         MP.SOURCE_TYPE                      MP_SOURCE_TYPE,
         MP.ORG_MAX_WEIGHT                   MP_ORG_MAX_WEIGHT,
         MP.ORG_MAX_WEIGHT_UOM_CODE          MP_ORG_MAX_WEIGHT_UOM_CODE,
         MP.ORG_MAX_VOLUME                   MP_ORG_MAX_VOLUME,
         MP.ORG_MAX_VOLUME_UOM_CODE          MP_ORG_MAX_VOLUME_UOM_CODE,
         MP.SERIAL_NUMBER_TYPE               MP_SERIAL_NUMBER_TYPE,
         MP.AUTO_SERIAL_ALPHA_PREFIX         MP_AUTO_SERIAL_ALPHA_PREFIX,
         MP.START_AUTO_SERIAL_NUMBER         MP_START_AUTO_SERIAL_NUMBER,
         MP.AUTO_LOT_ALPHA_PREFIX            MP_AUTO_LOT_ALPHA_PREFIX,
         MP.LOT_NUMBER_UNIQUENESS            MP_LOT_NUMBER_UNIQUENESS,
         MP.LOT_NUMBER_GENERATION            MP_LOT_NUMBER_GENERATION,
         MP.LOT_NUMBER_ZERO_PADDING          MP_LOT_NUMBER_ZERO_PADDING,
         MP.LOT_NUMBER_LENGTH                MP_LOT_NUMBER_LENGTH,
         MP.STARTING_REVISION                MP_STARTING_REVISION,
         MP.ATTRIBUTE_CATEGORY               MP_ATTRIBUTE_CATEGORY,
         MP.ATTRIBUTE1                       MP_ATTRIBUTE1,
         MP.ATTRIBUTE2                       MP_ATTRIBUTE2,
         MP.ATTRIBUTE3                       MP_ATTRIBUTE3,
         MP.ATTRIBUTE4                       MP_ATTRIBUTE4,
         MP.ATTRIBUTE5                       MP_ATTRIBUTE5,
         MP.ATTRIBUTE6                       MP_ATTRIBUTE6,
         MP.ATTRIBUTE7                       MP_ATTRIBUTE7,
         MP.ATTRIBUTE8                       MP_ATTRIBUTE8,
         MP.ATTRIBUTE9                       MP_ATTRIBUTE9,
         MP.ATTRIBUTE10                      MP_ATTRIBUTE10,
         MP.ATTRIBUTE11                      MP_ATTRIBUTE11,
         MP.ATTRIBUTE12                      MP_ATTRIBUTE12,
         MP.ATTRIBUTE13                      MP_ATTRIBUTE13,
         MP.ATTRIBUTE14                      MP_ATTRIBUTE14,
         MP.ATTRIBUTE15                      MP_ATTRIBUTE15,
         MP.DEFAULT_DEMAND_CLASS             MP_DEFAULT_DEMAND_CLASS,
         MP.ENCUMBRANCE_REVERSAL_FLAG        MP_ENCUMBRANCE_REVERSAL_FLAG,
         MP.MAINTAIN_FIFO_QTY_STACK_TYPE     MP_MAINTAIN_FIFO_QTY_STACK_TYPE,
         MP.INVOICE_PRICE_VAR_ACCOUNT        MP_INVOICE_PRICE_VAR_ACCOUNT,
         MP.AVERAGE_COST_VAR_ACCOUNT         MP_AVERAGE_COST_VAR_ACCOUNT,
         MP.SALES_ACCOUNT                    MP_SALES_ACCOUNT,
         MP.EXPENSE_ACCOUNT                  MP_EXPENSE_ACCOUNT,
         MP.SERIAL_NUMBER_GENERATION         MP_SERIAL_NUMBER_GENERATION,
         MP.REQUEST_ID                       MP_REQUEST_ID,
         MP.PROGRAM_APPLICATION_ID           MP_PROGRAM_APPLICATION_ID,
         MP.PROGRAM_ID                       MP_PROGRAM_ID,
         MP.PROGRAM_UPDATE_DATE              MP_PROGRAM_UPDATE_DATE,
         MP.GLOBAL_ATTRIBUTE_CATEGORY        MP_GLOBAL_ATTRIBUTE_CATEGORY,
         MP.GLOBAL_ATTRIBUTE1                MP_GLOBAL_ATTRIBUTE1,
         MP.GLOBAL_ATTRIBUTE2                MP_GLOBAL_ATTRIBUTE2,
         MP.GLOBAL_ATTRIBUTE3                MP_GLOBAL_ATTRIBUTE3,
         MP.GLOBAL_ATTRIBUTE4                MP_GLOBAL_ATTRIBUTE4,
         MP.GLOBAL_ATTRIBUTE5                MP_GLOBAL_ATTRIBUTE5,
         MP.GLOBAL_ATTRIBUTE6                MP_GLOBAL_ATTRIBUTE6,
         MP.GLOBAL_ATTRIBUTE7                MP_GLOBAL_ATTRIBUTE7,
         MP.GLOBAL_ATTRIBUTE8                MP_GLOBAL_ATTRIBUTE8,
         MP.GLOBAL_ATTRIBUTE9                MP_GLOBAL_ATTRIBUTE9,
         MP.GLOBAL_ATTRIBUTE10               MP_GLOBAL_ATTRIBUTE10,
         flv.LOOKUP_TYPE                     FLV_EMR_ORIGINATOR_WH_LS_I3100_LOOKUP_TYPE ,
         flv.LANGUAGE                        FLV_EMR_ORIGINATOR_WH_LS_I3100_LANGUAGE ,
         flv.LOOKUP_CODE                     FLV_EMR_ORIGINATOR_WH_LS_I3100_LOOKUP_CODE ,
         flv.MEANING                         FLV_EMR_ORIGINATOR_WH_LS_I3100_MEANING ,
         flv.DESCRIPTION                     FLV_EMR_ORIGINATOR_WH_LS_I3100_DESCRIPTION ,
         flv.ENABLED_FLAG                    FLV_EMR_ORIGINATOR_WH_LS_I3100_ENABLED_FLAG ,
         flv.START_DATE_ACTIVE               FLV_EMR_ORIGINATOR_WH_LS_I3100_START_DATE_ACTIVE ,
         flv.END_DATE_ACTIVE                 FLV_EMR_ORIGINATOR_WH_LS_I3100_END_DATE_ACTIVE ,
         flv.CREATED_BY                      FLV_EMR_ORIGINATOR_WH_LS_I3100_CREATED_BY ,
         flv.CREATION_DATE                   FLV_EMR_ORIGINATOR_WH_LS_I3100_CREATION_DATE ,
         flv.LAST_UPDATED_BY                 FLV_EMR_ORIGINATOR_WH_LS_I3100_LAST_UPDATED_BY ,
         flv.LAST_UPDATE_LOGIN               FLV_EMR_ORIGINATOR_WH_LS_I3100_LAST_UPDATE_LOGIN ,
         flv.LAST_UPDATE_DATE                FLV_EMR_ORIGINATOR_WH_LS_I3100_LAST_UPDATE_DATE ,
         flv.SOURCE_LANG                     FLV_EMR_ORIGINATOR_WH_LS_I3100_SOURCE_LANG ,
         flv.SECURITY_GROUP_ID               FLV_EMR_ORIGINATOR_WH_LS_I3100_SECURITY_GROUP_ID ,
         flv.VIEW_APPLICATION_ID             FLV_EMR_ORIGINATOR_WH_LS_I3100_VIEW_APPLICATION_ID ,
         flv.TERRITORY_CODE                  FLV_EMR_ORIGINATOR_WH_LS_I3100_TERRITORY_CODE ,
         flv.ATTRIBUTE_CATEGORY              FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE_CATEGORY ,
         flv.ATTRIBUTE1                      FLV_EMR_ORIGINATOR_WH_LS_I3100_DFF_WAREHOUSE_TYPE_EIA ,
         flv.ATTRIBUTE2                      FLV_EMR_ORIGINATOR_WH_LS_I3100_DFF_REGION_EIA ,
         flv.ATTRIBUTE3                      FLV_EMR_ORIGINATOR_WH_LS_I3100_DFF_FLAG_ORACLE_IO_EIA ,
         flv.ATTRIBUTE4                      FLV_EMR_ORIGINATOR_WH_LS_I3100_DFF_PO_OR_IR_CYCLE_EIA ,
         flv.ATTRIBUTE5                      FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE5 ,
         flv.ATTRIBUTE6                      FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE6 ,
         flv.ATTRIBUTE7                      FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE7 ,
         flv.ATTRIBUTE8                      FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE8 ,
         flv.ATTRIBUTE9                      FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE9 ,
         flv.ATTRIBUTE10                     FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE10 ,
         flv.ATTRIBUTE11                     FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE11 ,
         flv.ATTRIBUTE12                     FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE12 ,
         flv.ATTRIBUTE13                     FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE13 ,
         flv.ATTRIBUTE14                     FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE14 ,
         flv.ATTRIBUTE15                     FLV_EMR_ORIGINATOR_WH_LS_I3100_ATTRIBUTE15 ,
         flv.TAG                             FLV_EMR_ORIGINATOR_WH_LS_I3100_TAG ,
         flv.LEAF_NODE                       FLV_EMR_ORIGINATOR_WH_LS_I3100_LEAF_NODE ,
         flv.ZD_EDITION_NAME                 FLV_EMR_ORIGINATOR_WH_LS_I3100_ZD_EDITION_NAME ,
         flv.ZD_SYNC                         FLV_EMR_ORIGINATOR_WH_LS_I3100_ZD_SYNC ,
         flv.FETCH_DATE                      FLV_EMR_ORIGINATOR_WH_LS_I3100_FETCH_DATE ,
         rownum                              ROW_NUMBER_ID,
         sysdate                             ROW_CREATION_DATE ,
         sysdate                             ROW_LAST_UPDATE_DATE,
         GSOB.CURRENCY_CODE                  GSOB_CURRENCY_CODE
      FROM steph_apps_org_organization_definitions_bz    ood
      JOIN steph_apps_HR_ALL_ORGANIZATION_UNITS_bz       haou
         ON ood.organization_id = haou.organization_id
      LEFT OUTER JOIN steph_apps_hr_operating_units_bz   hou
         ON ood.operating_unit  = hou.organization_id
      LEFT OUTER JOIN steph_apps_hr_locations_all_bz     hla
         ON haou.location_id    = hla.LOCATION_ID
      LEFT OUTER JOIN steph_apps_mtl_parameters_bz       mp
         ON ood.ORGANIZATION_ID = mp.ORGANIZATION_ID
      LEFT OUTER JOIN steph_apps_FND_LOOKUP_VALUES_bz    flv
         ON flv.lookup_type     = 'EMR ORIGINATOR WH LS I3100'
         AND flv.language       = 'US'
         AND flv.lookup_code    = to_char(ood.organization_code)
      LEFT OUTER JOIN steph_apps_gl_sets_of_books_bz     gsob
         ON hou.set_of_books_id = gsob.set_of_books_id
      WHERE ood.organization_id != 356 -- exclusion du niveau 'LVO' qui n'est pas une IO
         AND ood.organization_id != 85 -- exclusion du niveau 'XMS' qui n'est pas une IO
   );
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   BEGIN
      g_table     := 'IO_DETAILS_TEMP';
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         EXECUTE IMMEDIATE 'CREATE INDEX IO_DETAILS_TEMP_PK ON IO_DETAILS_TEMP(OOD_ORGANIZATION_ID)';
      g_status   := 'COMPLETED';
      g_etape    := '012 - CREATE INDEX' ;
      Write_Log_PROC;
   EXCEPTION
      WHEN OTHERS THEN
         IF SQLCODE != -955 THEN
            Raise;
         ELSE
            DBMS_OUTPUT.PUT_LINE('Index already exists ');
         END IF;
   END;

   g_table     := 'IO_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
   ---- add defautl values for LS KOREA and LS SINGAPORE, as do not exist into STEPH
   INSERT INTO IO_DETAILS_TEMP (
      OOD_ORGANIZATION_ID, OOD_BUSINESS_GROUP_ID, OOD_USER_DEFINITION_ENABLE_DATE,
      OOD_ORGANIZATION_CODE, OOD_ORGANIZATION_NAME, OOD_OPERATING_UNIT,
      HAOU_DATE_FROM, HAOU_NAME,
      HAOU_LAST_UPDATE_DATE, HAOU_LAST_UPDATED_BY, HAOU_LAST_UPDATE_LOGIN,
      HAOU_CREATED_BY, HAOU_CREATION_DATE, HAOU_OBJECT_VERSION_NUMBER,
      HOU_BUSINESS_GROUP_ID, HOU_ORGANIZATION_ID , HOU_NAME ,  HLA_COUNTRY,
      ROW_CREATION_DATE , ROW_LAST_UPDATE_DATE , GSOB_CURRENCY_CODE
   ) VALUES
   (
      99992, 0, TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      'KOR', 'LS KOREA', 99990,
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'), 'LS KOREA',
      TO_TIMESTAMP('08/01/15 12:31:24', 'DD/MM/YY HH24:MI:SS.FF9'), 10227, 64135079,
      10227, TO_TIMESTAMP('08/01/15 11:05:10', 'DD/MM/YY HH24:MI:SS.FF9'), 2, 0, 99990, 'LS KOREA', 'KR',
      sysdate , sysdate , 'KRW'
   );
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'IO_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
   INSERT INTO IO_DETAILS_TEMP (
      OOD_ORGANIZATION_ID, OOD_BUSINESS_GROUP_ID, OOD_USER_DEFINITION_ENABLE_DATE,
      OOD_ORGANIZATION_CODE, OOD_ORGANIZATION_NAME, OOD_OPERATING_UNIT,
      HAOU_DATE_FROM, HAOU_NAME,
      HAOU_LAST_UPDATE_DATE, HAOU_LAST_UPDATED_BY, HAOU_LAST_UPDATE_LOGIN,
      HAOU_CREATED_BY, HAOU_CREATION_DATE, HAOU_OBJECT_VERSION_NUMBER,
      HOU_BUSINESS_GROUP_ID, HOU_ORGANIZATION_ID , HOU_NAME , HLA_COUNTRY,
      ROW_CREATION_DATE , ROW_LAST_UPDATE_DATE , GSOB_CURRENCY_CODE
   ) VALUES (
      99993, 0, TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      'SGP', 'LS SINGAPORE', 99991,
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'), 'LS SINGAPORE',
      TO_TIMESTAMP('08/01/15 12:31:24', 'DD/MM/YY HH24:MI:SS.FF9'), 10227, 64135079,
      10227, TO_TIMESTAMP('08/01/15 11:05:10', 'DD/MM/YY HH24:MI:SS.FF9'), 2, 0, 99991, 'LS SINGAPORE' , 'SG',
      sysdate , sysdate , 'SGD'
   );
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'IO_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
   INSERT INTO IO_DETAILS_TEMP (
      OOD_ORGANIZATION_ID, OOD_BUSINESS_GROUP_ID, OOD_USER_DEFINITION_ENABLE_DATE,
      OOD_ORGANIZATION_CODE, OOD_ORGANIZATION_NAME, OOD_OPERATING_UNIT,
      HAOU_DATE_FROM, HAOU_NAME,
      HAOU_LAST_UPDATE_DATE, HAOU_LAST_UPDATED_BY, HAOU_LAST_UPDATE_LOGIN,
      HAOU_CREATED_BY, HAOU_CREATION_DATE, HAOU_OBJECT_VERSION_NUMBER,
      HOU_BUSINESS_GROUP_ID, HOU_ORGANIZATION_ID , HOU_NAME , HLA_COUNTRY,
      ROW_CREATION_DATE , ROW_LAST_UPDATE_DATE , GSOB_CURRENCY_CODE
   ) VALUES (
      99995, 0, TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      'AUS', 'LS AUSTRALIA', 99994,
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'), 'LS AUSTRALIA',
      TO_TIMESTAMP('08/01/15 12:31:24', 'DD/MM/YY HH24:MI:SS.FF9'), 10227, 64135079,
      10227, TO_TIMESTAMP('08/01/15 11:05:10', 'DD/MM/YY HH24:MI:SS.FF9'), 2, 0, 99994, 'LS AUSTRALIA' , 'AU',
      sysdate , sysdate , 'AUD'
   );
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'IO_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('IO_DETAILS_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

   EXCEPTION
      WHEN OTHERS THEN
      Exceptions_PROC;

         g_table     := v_procedure;
         g_date_deb  := sysdate;
         g_status    := 'WIP';
         g_etape     := $$plsql_line + 1  || ' - num error line'  ;
            ROLLBACK;
         g_status   :='COMPLETED';
         g_etape    := '111 - ROLLBACK';
         Write_Log_PROC;

         g_table    := v_procedure;
         g_date_deb := v_date_deb_proc;
         g_status   := 'END FAILED';
         g_etape    := '9992 - End PROC';
         Write_Log_PROC;

   END Recreate_IO_Details_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_OU_Details_Proc
* DESCRIPTION :  création table regroupant les informations liées à l'OU dans différentes tables oracle dans une seule table
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_OU_Details_Proc
IS
   v_procedure varchar2(100) := 'Recreate_OU_Details_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'OU_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE OU_DETAILS_TEMP';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_date_deb  := sysdate;
   g_table     := 'OU_DETAILS_TEMP';
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;

   INSERT INTO OU_DETAILS_TEMP (
      SELECT
         HOU.BUSINESS_GROUP_ID                     HOU_BUSINESS_GROUP_ID,
         HOU.ORGANIZATION_ID                       HOU_ORGANIZATION_ID,
         HOU.NAME                                  HOU_NAME,
         HOU.DATE_FROM                             HOU_DATE_FROM,
         HOU.DATE_TO                               HOU_DATE_TO,
         HOU.SHORT_CODE                            HOU_SHORT_CODE,
         HOU.SET_OF_BOOKS_ID                       HOU_SET_OF_BOOKS_ID,
         HOU.DEFAULT_LEGAL_CONTEXT_ID              HOU_DEFAULT_LEGAL_CONTEXT_ID,
         HOU.USABLE_FLAG                           HOU_USABLE_FLAG,
         HAOU.ORGANIZATION_ID                      HAOU_ORGANIZATION_ID,
         HAOU.BUSINESS_GROUP_ID                    HAOU_BUSINESS_GROUP_ID,
         HAOU.COST_ALLOCATION_KEYFLEX_ID           HAOU_COST_ALLOCATION_KEYFLEX_ID,
         HAOU.LOCATION_ID                          HAOU_LOCATION_ID,
         HAOU.SOFT_CODING_KEYFLEX_ID               HAOU_SOFT_CODING_KEYFLEX_ID,
         HAOU.DATE_FROM                            HAOU_DATE_FROM,
         HAOU.NAME                                 HAOU_NAME,
         HAOU.DATE_TO                              HAOU_DATE_TO,
         HAOU.INTERNAL_EXTERNAL_FLAG               HAOU_INTERNAL_EXTERNAL_FLAG,
         HAOU.INTERNAL_ADDRESS_LINE                HAOU_INTERNAL_ADDRESS_LINE,
         HAOU.TYPE                                 HAOU_TYPE,
         HAOU.REQUEST_ID                           HAOU_REQUEST_ID,
         HAOU.PROGRAM_APPLICATION_ID               HAOU_PROGRAM_APPLICATION_ID,
         HAOU.PROGRAM_ID                           HAOU_PROGRAM_ID,
         HAOU.PROGRAM_UPDATE_DATE                  HAOU_PROGRAM_UPDATE_DATE,
         HAOU.ATTRIBUTE_CATEGORY                   HAOU_ATTRIBUTE_CATEGORY,
         HAOU.ATTRIBUTE1                           HAOU_ATTRIBUTE1,
         HAOU.ATTRIBUTE2                           HAOU_ATTRIBUTE2,
         HAOU.ATTRIBUTE3                           HAOU_ATTRIBUTE3,
         HAOU.ATTRIBUTE4                           HAOU_ATTRIBUTE4,
         HAOU.ATTRIBUTE5                           HAOU_ATTRIBUTE5,
         HAOU.ATTRIBUTE6                           HAOU_ATTRIBUTE6,
         HAOU.ATTRIBUTE7                           HAOU_ATTRIBUTE7,
         HAOU.ATTRIBUTE8                           HAOU_ATTRIBUTE8,
         HAOU.ATTRIBUTE9                           HAOU_ATTRIBUTE9,
         HAOU.ATTRIBUTE10                          HAOU_ATTRIBUTE10,
         HAOU.ATTRIBUTE11                          HAOU_ATTRIBUTE11,
         HAOU.ATTRIBUTE12                          HAOU_ATTRIBUTE12,
         HAOU.ATTRIBUTE13                          HAOU_ATTRIBUTE13,
         HAOU.ATTRIBUTE14                          HAOU_ATTRIBUTE14,
         HAOU.ATTRIBUTE15                          HAOU_ATTRIBUTE15,
         HAOU.ATTRIBUTE16                          HAOU_ATTRIBUTE16,
         HAOU.ATTRIBUTE17                          HAOU_ATTRIBUTE17,
         HAOU.ATTRIBUTE18                          HAOU_ATTRIBUTE18,
         HAOU.ATTRIBUTE19                          HAOU_ATTRIBUTE19,
         HAOU.ATTRIBUTE20                          HAOU_ATTRIBUTE20,
         HAOU.LAST_UPDATE_DATE                     HAOU_LAST_UPDATE_DATE,
         HAOU.LAST_UPDATED_BY                      HAOU_LAST_UPDATED_BY,
         HAOU.LAST_UPDATE_LOGIN                    HAOU_LAST_UPDATE_LOGIN,
         HAOU.CREATED_BY                           HAOU_CREATED_BY,
         HAOU.CREATION_DATE                        HAOU_CREATION_DATE,
         HAOU.OBJECT_VERSION_NUMBER                HAOU_OBJECT_VERSION_NUMBER,
         HAOU.PARTY_ID                             HAOU_PARTY_ID,
         HAOU.COMMENTS                             HAOU_COMMENTS,
         HAOU.ATTRIBUTE21                          HAOU_ATTRIBUTE21,
         HAOU.ATTRIBUTE22                          HAOU_ATTRIBUTE22,
         HAOU.ATTRIBUTE23                          HAOU_ATTRIBUTE23,
         HAOU.ATTRIBUTE24                          HAOU_ATTRIBUTE24,
         HAOU.ATTRIBUTE25                          HAOU_ATTRIBUTE25,
         HAOU.ATTRIBUTE26                          HAOU_ATTRIBUTE26,
         HAOU.ATTRIBUTE27                          HAOU_ATTRIBUTE27,
         HAOU.ATTRIBUTE28                          HAOU_ATTRIBUTE28,
         HAOU.ATTRIBUTE29                          HAOU_ATTRIBUTE29,
         HAOU.ATTRIBUTE30                          HAOU_ATTRIBUTE30,
         HLA.LOCATION_ID                           HLA_LOCATION_ID,
         HLA.LOCATION_CODE                         HLA_LOCATION_CODE,
         HLA.BUSINESS_GROUP_ID                     HLA_BUSINESS_GROUP_ID,
         HLA.DESCRIPTION                           HLA_DESCRIPTION,
         HLA.SHIP_TO_LOCATION_ID                   HLA_SHIP_TO_LOCATION_ID,
         HLA.SHIP_TO_SITE_FLAG                     HLA_SHIP_TO_SITE_FLAG,
         HLA.RECEIVING_SITE_FLAG                   HLA_RECEIVING_SITE_FLAG,
         HLA.BILL_TO_SITE_FLAG                     HLA_BILL_TO_SITE_FLAG,
         HLA.IN_ORGANIZATION_FLAG                  HLA_IN_ORGANIZATION_FLAG,
         HLA.OFFICE_SITE_FLAG                      HLA_OFFICE_SITE_FLAG,
         HLA.DESIGNATED_RECEIVER_ID                HLA_DESIGNATED_RECEIVER_ID,
         HLA.INVENTORY_ORGANIZATION_ID             HLA_INVENTORY_ORGANIZATION_ID,
         HLA.TAX_NAME                              HLA_TAX_NAME,
         HLA.INACTIVE_DATE                         HLA_INACTIVE_DATE,
         HLA.STYLE                                 HLA_STYLE,
         HLA.ADDRESS_LINE_1                        HLA_ADDRESS_LINE_1,
         HLA.ADDRESS_LINE_2                        HLA_ADDRESS_LINE_2,
         HLA.ADDRESS_LINE_3                        HLA_ADDRESS_LINE_3,
         HLA.TOWN_OR_CITY                          HLA_TOWN_OR_CITY,
         HLA.COUNTRY                               HLA_COUNTRY,
         HLA.POSTAL_CODE                           HLA_POSTAL_CODE,
         HLA.REGION_1                              HLA_REGION_1,
         HLA.REGION_2                              HLA_REGION_2,
         HLA.REGION_3                              HLA_REGION_3,
         HLA.TELEPHONE_NUMBER_1                    HLA_TELEPHONE_NUMBER_1,
         HLA.TELEPHONE_NUMBER_2                    HLA_TELEPHONE_NUMBER_2,
         HLA.TELEPHONE_NUMBER_3                    HLA_TELEPHONE_NUMBER_3,
         HLA.LOC_INFORMATION13                     HLA_LOC_INFORMATION13,
         HLA.LOC_INFORMATION14                     HLA_LOC_INFORMATION14,
         HLA.LOC_INFORMATION15                     HLA_LOC_INFORMATION15,
         HLA.LOC_INFORMATION16                     HLA_LOC_INFORMATION16,
         HLA.LOC_INFORMATION17                     HLA_LOC_INFORMATION17,
         HLA.ATTRIBUTE_CATEGORY                    HLA_ATTRIBUTE_CATEGORY,
         HLA.ATTRIBUTE1                            HLA_ATTRIBUTE1,
         HLA.ATTRIBUTE2                            HLA_ATTRIBUTE2,
         HLA.ATTRIBUTE3                            HLA_ATTRIBUTE3,
         HLA.ATTRIBUTE4                            HLA_ATTRIBUTE4,
         HLA.ATTRIBUTE5                            HLA_ATTRIBUTE5,
         HLA.ATTRIBUTE6                            HLA_ATTRIBUTE6,
         HLA.ATTRIBUTE7                            HLA_ATTRIBUTE7,
         HLA.ATTRIBUTE8                            HLA_ATTRIBUTE8,
         HLA.ATTRIBUTE9                            HLA_ATTRIBUTE9,
         HLA.ATTRIBUTE10                           HLA_ATTRIBUTE10,
         HLA.ATTRIBUTE11                           HLA_ATTRIBUTE11,
         HLA.ATTRIBUTE12                           HLA_ATTRIBUTE12,
         HLA.ATTRIBUTE13                           HLA_ATTRIBUTE13,
         HLA.ATTRIBUTE14                           HLA_ATTRIBUTE14,
         HLA.ATTRIBUTE15                           HLA_ATTRIBUTE15,
         HLA.ATTRIBUTE16                           HLA_ATTRIBUTE16,
         HLA.ATTRIBUTE17                           HLA_ATTRIBUTE17,
         HLA.ATTRIBUTE18                           HLA_ATTRIBUTE18,
         HLA.ATTRIBUTE19                           HLA_ATTRIBUTE19,
         HLA.ATTRIBUTE20                           HLA_ATTRIBUTE20,
         HLA.GLOBAL_ATTRIBUTE_CATEGORY             HLA_GLOBAL_ATTRIBUTE_CATEGORY,
         HLA.GLOBAL_ATTRIBUTE1                     HLA_GLOBAL_ATTRIBUTE1,
         HLA.GLOBAL_ATTRIBUTE2                     HLA_GLOBAL_ATTRIBUTE2,
         HLA.GLOBAL_ATTRIBUTE3                     HLA_GLOBAL_ATTRIBUTE3,
         HLA.GLOBAL_ATTRIBUTE4                     HLA_GLOBAL_ATTRIBUTE4,
         HLA.GLOBAL_ATTRIBUTE5                     HLA_GLOBAL_ATTRIBUTE5,
         HLA.GLOBAL_ATTRIBUTE6                     HLA_GLOBAL_ATTRIBUTE6,
         HLA.GLOBAL_ATTRIBUTE7                     HLA_GLOBAL_ATTRIBUTE7,
         HLA.GLOBAL_ATTRIBUTE8                     HLA_GLOBAL_ATTRIBUTE8,
         HLA.GLOBAL_ATTRIBUTE9                     HLA_GLOBAL_ATTRIBUTE9,
         HLA.GLOBAL_ATTRIBUTE10                    HLA_GLOBAL_ATTRIBUTE10,
         HLA.GLOBAL_ATTRIBUTE11                    HLA_GLOBAL_ATTRIBUTE11,
         HLA.GLOBAL_ATTRIBUTE12                    HLA_GLOBAL_ATTRIBUTE12,
         HLA.GLOBAL_ATTRIBUTE13                    HLA_GLOBAL_ATTRIBUTE13,
         HLA.GLOBAL_ATTRIBUTE14                    HLA_GLOBAL_ATTRIBUTE14,
         HLA.GLOBAL_ATTRIBUTE15                    HLA_GLOBAL_ATTRIBUTE15,
         HLA.GLOBAL_ATTRIBUTE16                    HLA_GLOBAL_ATTRIBUTE16,
         HLA.GLOBAL_ATTRIBUTE17                    HLA_GLOBAL_ATTRIBUTE17,
         HLA.GLOBAL_ATTRIBUTE18                    HLA_GLOBAL_ATTRIBUTE18,
         HLA.GLOBAL_ATTRIBUTE19                    HLA_GLOBAL_ATTRIBUTE19,
         HLA.GLOBAL_ATTRIBUTE20                    HLA_GLOBAL_ATTRIBUTE20,
         HLA.LAST_UPDATE_DATE                      HLA_LAST_UPDATE_DATE,
         HLA.LAST_UPDATED_BY                       HLA_LAST_UPDATED_BY,
         HLA.LAST_UPDATE_LOGIN                     HLA_LAST_UPDATE_LOGIN,
         HLA.CREATED_BY                            HLA_CREATED_BY,
         HLA.CREATION_DATE                         HLA_CREATION_DATE,
         HLA.ENTERED_BY                            HLA_ENTERED_BY,
         HLA.TP_HEADER_ID                          HLA_TP_HEADER_ID,
         HLA.ECE_TP_LOCATION_CODE                  HLA_ECE_TP_LOCATION_CODE,
         HLA.OBJECT_VERSION_NUMBER                 HLA_OBJECT_VERSION_NUMBER,
         HLA.GEOMETRY                              HLA_GOEMETRY,
         HLA.LOC_INFORMATION18                     HLA_LOC_INFORMATION18,
         HLA.LOC_INFORMATION19                     HLA_LOC_INFORMATION19,
         HLA.LOC_INFORMATION20                     HLA_LOC_INFORMATION20,
         HLA.DERIVED_LOCALE                        HLA_DERIVED_LOCALE ,
         HLA.LEGAL_ADDRESS_FLAG                    HLA_LEGAL_ADDRESS_FLAG,
         HLA.TIMEZONE_CODE                         HLA_TIMEZONE_CODE,
         GSOB.SET_OF_BOOKS_ID                      GSOB_SET_OF_BOOKS_ID,
         GSOB.NAME                                 GSOB_NAME,
         GSOB.SHORT_NAME                           GSOB_SHORT_NAME,
         GSOB.CHART_OF_ACCOUNTS_ID                 GSOB_CHART_OF_ACCOUNTS_ID,
         GSOB.CURRENCY_CODE                        GSOB_CURRENCY_CODE,
         GSOB.PERIOD_SET_NAME                      GSOB_PERIOD_SET_NAME,
         GSOB.ACCOUNTED_PERIOD_TYPE                GSOB_ACCOUNTED_PERIOD_TYPE,
         GSOB.SUSPENSE_ALLOWED_FLAG                GSOB_SUSPENSE_ALLOWED_FLAG,
         GSOB.ALLOW_INTERCOMPANY_POST_FLAG         GSOB_ALLOW_INTERCOMPANY_POST_FLAG,
         GSOB.TRACK_ROUNDING_IMBALANCE_FLAG        GSOB_TRACK_ROUNDING_IMBALANCE_FLAG,
         GSOB.ENABLE_AVERAGE_BALANCES_FLAG         GSOB_ENABLE_AVERAGE_BALANCES_FLAG,
         GSOB.ENABLE_BUDGETARY_CONTROL_FLAG        GSOB_ENABLE_BUDGETARY_CONTROL_FLAG,
         GSOB.REQUIRE_BUDGET_JOURNALS_FLAG         GSOB_REQUIRE_BUDGET_JOURNALS_FLAG,
         GSOB.ENABLE_JE_APPROVAL_FLAG              GSOB_ENABLE_JE_APPROVAL_FLAG,
         GSOB.ENABLE_AUTOMATIC_TAX_FLAG            GSOB_ENABLE_AUTOMATIC_TAX_FLAG,
         GSOB.CONSOLIDATION_SOB_FLAG               GSOB_CONSOLIDATION_SOB_FLAG,
         GSOB.TRANSLATE_EOD_FLAG                   GSOB_TRANSLATE_EOD_FLAG,
         GSOB.TRANSLATE_QATD_FLAG                  GSOB_TRANSLATE_QATD_FLAG,
         GSOB.TRANSLATE_YATD_FLAG                  GSOB_TRANSLATE_YATD_FLAG,
         GSOB.MRC_SOB_TYPE_CODE                    GSOB_MRC_SOB_TYPE_CODE,
         GSOB.ALLOW_POSTING_WARNING_FLAG           GSOB_ALLOW_POSTING_WARNING_FLAG,
         GSOB.LAST_UPDATE_DATE                     GSOB_LAST_UPDATE_DATE,
         GSOB.LAST_UPDATED_BY                      GSOB_LAST_UPDATED_BY,
         GSOB.CREATION_DATE                        GSOB_CREATION_DATE,
         GSOB.CREATED_BY                           GSOB_CREATED_BY,
         GSOB.LAST_UPDATE_LOGIN                    GSOB_LAST_UPDATE_LOGIN,
         GSOB.FUTURE_ENTERABLE_PERIODS_LIMIT       GSOB_FUTURE_ENTERABLE_PERIODS_LIMIT,
         GSOB.LATEST_OPENED_PERIOD_NAME            GSOB_LATEST_OPENED_PERIOD_NAME,
         GSOB.LATEST_ENCUMBRANCE_YEAR              GSOB_LATEST_ENCUMBRANCE_YEAR,
         GSOB.RET_EARN_CODE_COMBINATION_ID         GSOB_RET_EARN_CODE_COMBINATION_ID,
         GSOB.CUM_TRANS_CODE_COMBINATION_ID        GSOB_CUM_TRANS_CODE_COMBINATION_ID,
         GSOB.RES_ENCUMB_CODE_COMBINATION_ID       GSOB_RES_ENCUMB_CODE_COMBINATION_ID,
         GSOB.NET_INCOME_CODE_COMBINATION_ID       GSOB_NET_INCOME_CODE_COMBINATION_ID,
         GSOB.ROUNDING_CODE_COMBINATION_ID         GSOB_ROUNDING_CODE_COMBINATION_ID,
         GSOB.TRANSACTION_CALENDAR_ID              GSOB_TRANSACTION_CALENDAR_ID,
         GSOB.DAILY_TRANSLATION_RATE_TYPE          GSOB_DAILY_TRANSLATION_RATE_TYPE,
         GSOB.EARLIEST_UNTRANS_PERIOD_NAME         GSOB_EARLIEST_UNTRANS_PERIOD_NAME,
         GSOB.DESCRIPTION                          GSOB_DESCRIPTION,
         GSOB.ATTRIBUTE1                           GSOB_ATTRIBUTE1,
         GSOB.ATTRIBUTE2                           GSOB_ATTRIBUTE2,
         GSOB.ATTRIBUTE3                           GSOB_ATTRIBUTE3,
         GSOB.ATTRIBUTE4                           GSOB_ATTRIBUTE4,
         GSOB.ATTRIBUTE5                           GSOB_ATTRIBUTE5,
         GSOB.ATTRIBUTE6                           GSOB_ATTRIBUTE6,
         GSOB.ATTRIBUTE7                           GSOB_ATTRIBUTE7,
         GSOB.ATTRIBUTE8                           GSOB_ATTRIBUTE8,
         GSOB.ATTRIBUTE9                           GSOB_ATTRIBUTE9,
         GSOB.ATTRIBUTE10                          GSOB_ATTRIBUTE10,
         GSOB.ATTRIBUTE11                          GSOB_ATTRIBUTE11,
         GSOB.ATTRIBUTE12                          GSOB_ATTRIBUTE12,
         GSOB.ATTRIBUTE13                          GSOB_ATTRIBUTE13,
         GSOB.ATTRIBUTE14                          GSOB_ATTRIBUTE14,
         GSOB.ATTRIBUTE15                          GSOB_ATTRIBUTE15,
         GSOB.CONTEXT                              GSOB_CONTEXT,
         GSOB.GLOBAL_ATTRIBUTE_CATEGORY            GSOB_GLOBAL_ATTRIBUTE_CATEGORY,
         GSOB.GLOBAL_ATTRIBUTE1                    GSOB_GLOBAL_ATTRIBUTE1,
         GSOB.GLOBAL_ATTRIBUTE2                    GSOB_GLOBAL_ATTRIBUTE2,
         GSOB.GLOBAL_ATTRIBUTE3                    GSOB_GLOBAL_ATTRIBUTE3,
         GSOB.GLOBAL_ATTRIBUTE4                    GSOB_GLOBAL_ATTRIBUTE4,
         GSOB.GLOBAL_ATTRIBUTE5                    GSOB_GLOBAL_ATTRIBUTE5,
         GSOB.GLOBAL_ATTRIBUTE6                    GSOB_GLOBAL_ATTRIBUTE6,
         GSOB.GLOBAL_ATTRIBUTE7                    GSOB_GLOBAL_ATTRIBUTE7,
         GSOB.GLOBAL_ATTRIBUTE8                    GSOB_GLOBAL_ATTRIBUTE8,
         GSOB.GLOBAL_ATTRIBUTE9                    GSOB_GLOBAL_ATTRIBUTE9,
         GSOB.GLOBAL_ATTRIBUTE10                   GSOB_GLOBAL_ATTRIBUTE10,
         GSOB.SLA_LEDGER_CASH_BASIS_FLAG           GSOB_SLA_LEDGER_CASH_BASIS_FLAG,
         rownum                                    ROW_NUMBER_ID,
         sysdate                                   ROW_CREATION_DATE ,
         sysdate                                   ROW_LAST_UPDATE_DATE
      FROM steph_apps_hr_operating_units_bz hou
      LEFT JOIN steph_apps_HR_ALL_ORGANIZATION_UNITS_bz haou
         ON hou.organization_id = haou.organization_id
      LEFT JOIN steph_apps_hr_locations_all_bz hla
         ON haou.location_id    = hla.LOCATION_ID
      LEFT JOIN steph_apps_gl_sets_of_books_bz gsob
         ON hou.set_of_books_id = gsob.set_of_books_id
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   BEGIN
      g_table     := 'OU_DETAILS_TEMP';
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         EXECUTE IMMEDIATE 'CREATE INDEX OU_DETAILS_TEMP_PK ON OU_DETAILS_TEMP(HOU_ORGANIZATION_ID)';
      g_status   := 'COMPLETED';
      g_etape    := '012 - CREATE INDEX' ;
      Write_Log_PROC;
   EXCEPTION
      WHEN OTHERS THEN
         IF SQLCODE != -955 THEN
            Raise;
         ELSE
            DBMS_OUTPUT.PUT_LINE('Index already exists ');
         END IF;
   END;

   g_table     := 'OU_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
   ---- add defautl values for LS KOREA and LS SINGAPORE, as do not exist into STEPH
   INSERT INTO OU_DETAILS_TEMP (
         HAOU_DATE_FROM, HAOU_NAME,
         HAOU_LAST_UPDATE_DATE, HAOU_LAST_UPDATED_BY, HAOU_LAST_UPDATE_LOGIN,
         HAOU_CREATED_BY, HAOU_CREATION_DATE, HAOU_OBJECT_VERSION_NUMBER,
         HOU_BUSINESS_GROUP_ID, HOU_ORGANIZATION_ID, HOU_NAME , HLA_COUNTRY, GSOB_CURRENCY_CODE,
         ROW_CREATION_DATE , ROW_LAST_UPDATE_DATE
   ) VALUES
   (
         TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'), 'LS KOREA',
         TO_TIMESTAMP('08/01/15 12:31:24', 'DD/MM/YY HH24:MI:SS.FF9'), 10227, 64135079,
         10227, TO_TIMESTAMP('08/01/15 11:05:10', 'DD/MM/YY HH24:MI:SS.FF9'), 2, 0, 99990, 'LS KOREA', 'KR', 'KRW',
         sysdate , sysdate
   );
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'OU_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
   INSERT INTO OU_DETAILS_TEMP (
         HAOU_DATE_FROM, HAOU_NAME,
         HAOU_LAST_UPDATE_DATE, HAOU_LAST_UPDATED_BY, HAOU_LAST_UPDATE_LOGIN,
         HAOU_CREATED_BY, HAOU_CREATION_DATE, HAOU_OBJECT_VERSION_NUMBER,
         HOU_BUSINESS_GROUP_ID, HOU_ORGANIZATION_ID, HOU_NAME , HLA_COUNTRY, GSOB_CURRENCY_CODE,
         ROW_CREATION_DATE , ROW_LAST_UPDATE_DATE
   ) VALUES (
         TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'), 'LS SINGAPORE',
         TO_TIMESTAMP('08/01/15 12:31:24', 'DD/MM/YY HH24:MI:SS.FF9'), 10227, 64135079,
         10227, TO_TIMESTAMP('08/01/15 11:05:10', 'DD/MM/YY HH24:MI:SS.FF9'), 2, 0, 99991, 'LS SINGAPORE' , 'SG', 'SGD' ,
         sysdate , sysdate
   );
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'OU_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
   INSERT INTO OU_DETAILS_TEMP (
         HAOU_DATE_FROM, HAOU_NAME,
         HAOU_LAST_UPDATE_DATE, HAOU_LAST_UPDATED_BY, HAOU_LAST_UPDATE_LOGIN,
         HAOU_CREATED_BY, HAOU_CREATION_DATE, HAOU_OBJECT_VERSION_NUMBER,
         HOU_BUSINESS_GROUP_ID, HOU_ORGANIZATION_ID, HOU_NAME , HLA_COUNTRY, GSOB_CURRENCY_CODE,
         ROW_CREATION_DATE , ROW_LAST_UPDATE_DATE
   ) VALUES (
         TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'), 'LS AUSTRALIA',
         TO_TIMESTAMP('08/01/15 12:31:24', 'DD/MM/YY HH24:MI:SS.FF9'), 10227, 64135079,
         10227, TO_TIMESTAMP('08/01/15 11:05:10', 'DD/MM/YY HH24:MI:SS.FF9'), 2, 0, 99994, 'LS AUSTRALIA' , 'AU', 'AUD' ,
         sysdate , sysdate
   );
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'OU_DETAILS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('OU_DETAILS_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'   ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_OU_Details_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_Structure_Reseau_Proc, mix des tables oracle et AS400
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Structure_Reseau_Proc
IS
   v_procedure varchar2(100) := 'Recreate_Structure_Reseau_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'STRUCTURE_RESEAU_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE STRUCTURE_RESEAU_TEMP'  ;
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'STRUCTURE_RESEAU_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;

   INSERT INTO STRUCTURE_RESEAU_TEMP
   SELECT *
   FROM (
      SELECT DISTINCT
         jrs.SALESREP_ID                     ORA_SALESREP_SALESREP_ID,
         jrs.RESOURCE_ID                     ORA_SALESREP_RESOURCE_ID,
         jrs.LAST_UPDATE_DATE                ORA_SALESREP_LAST_UPDATE_DATE,
         jrs.LAST_UPDATED_BY                 ORA_SALESREP_LAST_UPDATED_BY,
         jrs.CREATION_DATE                   ORA_SALESREP_CREATION_DATE,
         jrs.CREATED_BY                      ORA_SALESREP_CREATED_BY,
         jrs.LAST_UPDATE_LOGIN               ORA_SALESREP_LAST_UPDATE_LOGIN,
         jrs.SALES_CREDIT_TYPE_ID            ORA_SALESREP_SALES_CREDIT_TYPE_ID,
         jrs.NAME                            ORA_SALESREP_NAME,
         jrs.STATUS                          ORA_SALESREP_STATUS,
         jrs.START_DATE_ACTIVE               ORA_SALESREP_START_DATE_ACTIVE,
         jrs.END_DATE_ACTIVE                 ORA_SALESREP_END_DATE_ACTIVE,
         jrs.GL_ID_REV                       ORA_SALESREP_GL_ID_REV,
         jrs.GL_ID_FREIGHT                   ORA_SALESREP_GL_ID_FREIGHT,
         jrs.GL_ID_REC                       ORA_SALESREP_GL_ID_REC,
         jrs.SET_OF_BOOKS_ID                 ORA_SALESREP_SET_OF_BOOKS_ID,
         jrs.SALESREP_NUMBER                 ORA_SALESREP_SALESREP_NUMBER,
         jrs.ORG_ID                          ORA_SALESREP_ORG_ID,
         jrs.EMAIL_ADDRESS                   ORA_SALESREP_EMAIL_ADDRESS,
         jrs.WH_UPDATE_DATE                  ORA_SALESREP_WH_UPDATE_DATE,
         jrs.PERSON_ID                       ORA_SALESREP_PERSON_ID,
         jrs.SALES_TAX_GEOCODE               ORA_SALESREP_SALES_TAX_GEOCODE,
         jrs.SALES_TAX_INSIDE_CITY_LIMITS    ORA_SALESREP_SALES_TAX_INSIDE_CITY_LIMITS,
         jrs.OBJECT_VERSION_NUMBER           ORA_SALESREP_OBJECT_VERSION_NUMBER,
         jrs.ATTRIBUTE_CATEGORY              ORA_SALESREP_ATTRIBUTE_CATEGORY,
         jrs.ATTRIBUTE1                      ORA_SALESREP_ATTRIBUTE1,
         jrs.ATTRIBUTE2                      ORA_SALESREP_ATTRIBUTE2,
         jrs.ATTRIBUTE3                      ORA_SALESREP_ATTRIBUTE3,
         jrs.ATTRIBUTE4                      ORA_SALESREP_ATTRIBUTE4,
         jrs.ATTRIBUTE5                      ORA_SALESREP_ATTRIBUTE5,
         jrs.ATTRIBUTE6                      ORA_SALESREP_ATTRIBUTE6,
         jrs.ATTRIBUTE7                      ORA_SALESREP_ATTRIBUTE7,
         jrs.ATTRIBUTE8                      ORA_SALESREP_ATTRIBUTE8,
         jrs.ATTRIBUTE9                      ORA_SALESREP_ATTRIBUTE9,
         jrs.ATTRIBUTE10                     ORA_SALESREP_ATTRIBUTE10,
         jrs.ATTRIBUTE11                     ORA_SALESREP_ATTRIBUTE11,
         jrs.ATTRIBUTE12                     ORA_SALESREP_ATTRIBUTE12,
         jrs.ATTRIBUTE13                     ORA_SALESREP_ATTRIBUTE13,
         jrs.ATTRIBUTE14                     ORA_SALESREP_ATTRIBUTE14,
         jrs.ATTRIBUTE15                     ORA_SALESREP_ATTRIBUTE15,
         jrs.SECURITY_GROUP_ID               ORA_SALESREP_SECURITY_GROUP_ID,
         jrs.FETCH_DATE                      ORA_SALESREP_FETCH_DATE,
         jrd.ROW_ID                          ORA_RESOURCE_ROW_ID,
         jrd.CREATED_BY                      ORA_RESOURCE_CREATED_BY,
         jrd.CREATION_DATE                   ORA_RESOURCE_CREATION_DATE,
         jrd.LAST_UPDATED_BY                 ORA_RESOURCE_LAST_UPDATED_BY,
         jrd.LAST_UPDATE_DATE                ORA_RESOURCE_LAST_UPDATE_DATE,
         jrd.LAST_UPDATE_LOGIN               ORA_RESOURCE_LAST_UPDATE_LOGIN,
         jrd.CATEGORY                        ORA_RESOURCE_CATEGORY,
         jrd.CATG_MEANING                    ORA_RESOURCE_CATG_MEANING,
         jrd.RESOURCE_NUMBER                 ORA_RESOURCE_ORA_RESOURCE_NUMBER,
         jrd.SOURCE_ID                       ORA_RESOURCE_SOURCE_ID,
         jrd.ADDRESS_ID                      ORA_RESOURCE_ADDRESS_ID,
         jrd.CONTACT_ID                      ORA_RESOURCE_CONTACT_ID,
         jrd.MANAGING_EMPLOYEE_ID            ORA_RESOURCE_MANAGING_EMPLOYEE_ID,
         jrd.START_DATE_ACTIVE               ORA_RESOURCE_START_DATE_ACTIVE,
         jrd.END_DATE_ACTIVE                 ORA_RESOURCE_END_DATE_ACTIVE,
         jrd.TIME_ZONE                       ORA_RESOURCE_TIME_ZONE,
         jrd.COST_PER_HR                     ORA_RESOURCE_COST_PER_HR,
         jrd.PRIMARY_LANGUAGE                ORA_RESOURCE_PRIMARY_LANGUAGE,
         jrd.SECONDARY_LANGUAGE              ORA_RESOURCE_SECONDARY_LANGUAGE,
         jrd.SUPPORT_SITE_ID                 ORA_RESOURCE_SUPPORT_SITE_ID,
         jrd.IES_AGENT_LOGIN                 ORA_RESOURCE_IES_AGENT_LOGIN,
         jrd.SERVER_GROUP_ID                 ORA_RESOURCE_SERVER_GROUP_ID,
         jrd.ASSIGNED_TO_GROUP_ID            ORA_RESOURCE_ASSIGNED_TO_GROUP_ID,
         jrd.COST_CENTER                     ORA_RESOURCE_COST_CENTER,
         jrd.CHARGE_TO_COST_CENTER           ORA_RESOURCE_CHARGE_TO_COST_CENTER,
         jrd.COMPENSATION_CURRENCY_CODE      ORA_RESOURCE_COMPENSATION_CURRENCY_CODE,
         jrd.COMMISSIONABLE_FLAG             ORA_RESOURCE_COMMISSIONABLE_FLAG,
         jrd.HOLD_REASON_CODE                ORA_RESOURCE_HOLD_REASON_CODE,
         jrd.HOLD_PAYMENT                    ORA_RESOURCE_HOLD_PAYMENT,
         jrd.COMP_SERVICE_TEAM_ID            ORA_RESOURCE_COMP_SERVICE_TEAM_ID,
         jrd.TRANSACTION_NUMBER              ORA_RESOURCE_TRANSACTION_NUMBER,
         jrd.OBJECT_VERSION_NUMBER           ORA_RESOURCE_OBJECT_VERSION_NUMBER,
         jrd.ATTRIBUTE1                      ORA_RESOURCE_ATTRIBUTE1,
         jrd.ATTRIBUTE2                      ORA_RESOURCE_ATTRIBUTE2,
         jrd.ATTRIBUTE3                      ORA_RESOURCE_ATTRIBUTE3,
         jrd.ATTRIBUTE4                      ORA_RESOURCE_ATTRIBUTE4,
         jrd.ATTRIBUTE5                      ORA_RESOURCE_ATTRIBUTE5,
         jrd.ATTRIBUTE6                      ORA_RESOURCE_ATTRIBUTE6,
         jrd.ATTRIBUTE7                      ORA_RESOURCE_ATTRIBUTE7,
         jrd.ATTRIBUTE8                      ORA_RESOURCE_ATTRIBUTE8,
         jrd.ATTRIBUTE9                      ORA_RESOURCE_ATTRIBUTE9,
         jrd.ATTRIBUTE10                     ORA_RESOURCE_ATTRIBUTE10,
         jrd.ATTRIBUTE11                     ORA_RESOURCE_ATTRIBUTE11,
         jrd.ATTRIBUTE12                     ORA_RESOURCE_ATTRIBUTE12,
         jrd.ATTRIBUTE13                     ORA_RESOURCE_ATTRIBUTE13,
         jrd.ATTRIBUTE14                     ORA_RESOURCE_ATTRIBUTE14,
         jrd.ATTRIBUTE15                     ORA_RESOURCE_ATTRIBUTE15,
         jrd.ATTRIBUTE_CATEGORY              ORA_RESOURCE_ATTRIBUTE_CATEGORY,
         jrd.USER_ID                         ORA_RESOURCE_USER_ID,
         jrd.RESOURCE_NAME                   ORA_RESOURCE_RESOURCE_NAME,
         jrd.SOURCE_NAME                     ORA_RESOURCE_SOURCE_NAME,
         jrd.SOURCE_NUMBER                   ORA_RESOURCE_SOURCE_NUMBER,
         jrd.SOURCE_JOB_TITLE                ORA_RESOURCE_SOURCE_JOB_TITLE,
         jrd.SOURCE_EMAIL                    ORA_RESOURCE_SOURCE_EMAIL,
         jrd.SOURCE_PHONE                    ORA_RESOURCE_SOURCE_PHONE,
         jrd.SOURCE_ORG_ID                   ORA_RESOURCE_SOURCE_ORG_ID,
         jrd.SOURCE_ORG_NAME                 ORA_RESOURCE_SOURCE_ORG_NAME,
         jrd.SOURCE_ADDRESS1                 ORA_RESOURCE_SOURCE_ADDRESS1,
         jrd.SOURCE_ADDRESS2                 ORA_RESOURCE_SOURCE_ADDRESS2,
         jrd.SOURCE_ADDRESS3                 ORA_RESOURCE_SOURCE_ADDRESS3,
         jrd.SOURCE_ADDRESS4                 ORA_RESOURCE_SOURCE_ADDRESS4,
         jrd.SOURCE_CITY                     ORA_RESOURCE_SOURCE_CITY,
         jrd.SOURCE_POSTAL_CODE              ORA_RESOURCE_SOURCE_POSTAL_CODE,
         jrd.SOURCE_STATE                    ORA_RESOURCE_SOURCE_STATE,
         jrd.SOURCE_PROVINCE                 ORA_RESOURCE_SOURCE_PROVINCE,
         jrd.SOURCE_COUNTY                   ORA_RESOURCE_SOURCE_COUNTY,
         jrd.SOURCE_COUNTRY                  ORA_RESOURCE_SOURCE_COUNTRY,
         jrd.SOURCE_MGR_ID                   ORA_RESOURCE_SOURCE_MGR_ID,
         jrd.SOURCE_MGR_NAME                 ORA_RESOURCE_SOURCE_MGR_NAME,
         jrd.SOURCE_BUSINESS_GRP_ID          ORA_RESOURCE_SOURCE_BUSINESS_GRP_ID,
         jrd.SOURCE_BUSINESS_GRP_NAME        ORA_RESOURCE_SOURCE_BUSINESS_GRP_NAME,
         jrd.SOURCE_FIRST_NAME               ORA_RESOURCE_SOURCE_FIRST_NAME,
         jrd.SOURCE_MIDDLE_NAME              ORA_RESOURCE_SOURCE_MIDDLE_NAME,
         jrd.SOURCE_LAST_NAME                ORA_RESOURCE_SOURCE_LAST_NAME,
         jrd.SOURCE_CATEGORY                 ORA_RESOURCE_SOURCE_CATEGORY,
         jrd.SOURCE_STATUS                   ORA_RESOURCE_SOURCE_STATUS,
         jrd.USER_NAME                       ORA_RESOURCE_USER_NAME,
         jrd.SOURCE_OFFICE                   ORA_RESOURCE_SOURCE_OFFICE,
         jrd.SOURCE_LOCATION                 ORA_RESOURCE_SOURCE_LOCATION,
         jrd.SOURCE_MAILSTOP                 ORA_RESOURCE_SOURCE_MAILSTOP,
         jrd.SOURCE_MOBILE_PHONE             ORA_RESOURCE_SOURCE_MOBILE_PHONE,
         jrd.SOURCE_PAGER                    ORA_RESOURCE_SOURCE_PAGER,
         jrd.FETCH_DATE                      ORA_RESOURCE_FETCH_DATE,
         flv.LOOKUP_TYPE                     ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_LOOKUP_TYPE,
         flv.LANGUAGE                        ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_LANGUAGE,
         flv.LOOKUP_CODE                     ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_LOOKUP_CODE,
         flv.MEANING                         ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_MEANING,
         flv.DESCRIPTION                     ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_DESCRIPTION,
         flv.ENABLED_FLAG                    ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ENABLED_FLAG,
         flv.START_DATE_ACTIVE               ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_START_DATE_ACTIVE,
         flv.END_DATE_ACTIVE                 ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_END_DATE_ACTIVE,
         flv.CREATED_BY                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_CREATED_BY,
         flv.CREATION_DATE                   ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_CREATION_DATE,
         flv.LAST_UPDATED_BY                 ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_LAST_UPDATED_BY,
         flv.LAST_UPDATE_LOGIN               ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_LAST_UPDATE_LOGIN,
         flv.LAST_UPDATE_DATE                ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_LAST_UPDATE_DATE,
         flv.SOURCE_LANG                     ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_SOURCE_LANG,
         flv.SECURITY_GROUP_ID               ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_SECURITY_GROUP_ID,
         flv.VIEW_APPLICATION_ID             ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_VIEW_APPLICATION_ID,
         flv.TERRITORY_CODE                  ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_TERRITORY_CODE,
         flv.ATTRIBUTE_CATEGORY              ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE_CATEGORY,
         flv.ATTRIBUTE1                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE1,
         flv.ATTRIBUTE2                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE2,
         flv.ATTRIBUTE3                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE3,
         flv.ATTRIBUTE4                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE4,
         flv.ATTRIBUTE5                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE5,
         flv.ATTRIBUTE6                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE6,
         flv.ATTRIBUTE7                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE7,
         flv.ATTRIBUTE8                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE8,
         flv.ATTRIBUTE9                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE9,
         flv.ATTRIBUTE10                     ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE10,
         flv.ATTRIBUTE11                     ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE11,
         flv.ATTRIBUTE12                     ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE12,
         flv.ATTRIBUTE13                     ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE13,
         flv.ATTRIBUTE14                     ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE14,
         flv.ATTRIBUTE15                     ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ATTRIBUTE15,
         flv.TAG                             ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_TAG,
         flv.LEAF_NODE                       ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_LEAF_NODE,
         flv.ZD_EDITION_NAME                 ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ZD_EDITION_NAME,
         flv.ZD_SYNC                         ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_ZD_SYNC,
         flv.FETCH_DATE                      ORA_LOOKUP_EMR_BRANCH_AREA_MAPPING_MLS_FETCH_DATE,
         ag.WAGAGT                           AS400_AGENT,
         ag.WAGLBAGT                         AS400_AGENT_LIBELLE,
         ag.WAGMTV                           AS400_METIER_VENTE,
         ag.WAGSUC                           AS400_SUCCURSALE,
         su.WSULBSUC                         AS400_SUCCURSALE_LIBELLE,
         su.WSUREG                           AS400_REGION,
         rg.WRGLBREG                         AS400_REGION_LIBELLE,
         rg.WRGMAR                           AS400_MARCHE,
         mr.WMRLBMAR                         AS400_MARCHE_LIBELLE,
         mr.FETCH_DATE                       AS400_FETCH_DATE ,
         rownum                              ROW_NUMBER_ID,
         sysdate                             ROW_CREATION_DATE ,
         sysdate                             ROW_LAST_UPDATE_DATE,
         to_char(jrs.salesrep_id)            ORA_SALESREP_SALESREP_IDA
      FROM steph_apps_JTF_RS_SALESREPS_bz jrs
      LEFT OUTER JOIN steph_apps_JTF_RS_DEFRESOURCES_V_bz jrd
         ON jrs.resource_id = jrd.resource_id
      LEFT OUTER JOIN steph_apps_FND_LOOKUP_VALUES_bz flv
         ON jrd.attribute1 = flv.lookup_code
         AND jrd.ATTRIBUTE_CATEGORY in ('LEROY SOMER', 'EIA')
         AND flv.LANGUAGE = 'US'
         AND flv.LOOKUP_TYPE = 'EMR BRANCH AREA MAPPING MLS'
      LEFT OUTER JOIN siege_lbprddwh_pfwag_bz ag
         ON jrs.salesrep_number = to_char(ag.wagagt)
      LEFT OUTER JOIN siege_lbprddwh_pfwsu_bz su
         ON ag.wagsuc = su.wsusuc
      LEFT OUTER JOIN siege_lbprddwh_pfwrg_bz rg
         ON su.wsureg = rg.wrgreg
      LEFT OUTER JOIN siege_lbprddwh_pfwmr_bz mr
         ON rg.wrgmar = mr.wmrmar
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'STRUCTURE_RESEAU_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('STRUCTURE_RESEAU_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   BEGIN
      g_table     := 'STRUCTURE_RESEAU_TEMP';
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         EXECUTE IMMEDIATE 'CREATE INDEX STRUCTURE_RESEAU_TEMP_PK ON STRUCTURE_RESEAU_TEMP(ORA_SALESREP_SALESREP_ID , ORA_SALESREP_ORG_ID)';
      g_status   := 'COMPLETED';
      g_etape    := '012 - CREATE INDEX' ;
      Write_Log_PROC;
   EXCEPTION
      WHEN OTHERS THEN
         IF SQLCODE != -955 THEN
            Raise;
         ELSE
            DBMS_OUTPUT.PUT_LINE('Index already exists ');
         END IF;
   END;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_Structure_Reseau_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_Intercompany_Parameters_Proc
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Intercompany_Parameters_Proc
IS
   v_procedure varchar2(100) := 'Recreate_Intercompany_Parameters_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'STEPH_INTERCOMPANY_PARAMETERS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE INTERCOMPANY_PARAMETERS_TEMP'  ;
   g_status    := 'COMPLETED';
   g_etape     := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'INTERCOMPANY_PARAMETERS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
   INSERT INTO INTERCOMPANY_PARAMETERS_TEMP
   SELECT
      t.* ,
      rownum                      ROW_NUMBER_ID,
      sysdate                     ROW_CREATION_DATE ,
      sysdate                     ROW_LAST_UPDATE_DATE
   FROM  STEPH_APPS_MTL_INTERCOMPANY_PARAMETERS_BZ t
   ;
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'INTERCOMPANY_PARAMETERS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('INTERCOMPANY_PARAMETERS_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'   ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_Intercompany_Parameters_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_Gl_Segment1_Proc
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Gl_Segment1_Proc
IS
   v_procedure varchar2(100) := 'Recreate_Gl_Segment1_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT1_BU_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
      EXECUTE IMMEDIATE 'TRUNCATE TABLE GL_SEGMENT1_BU_TEMP';
   g_status    := 'COMPLETED';
   g_etape     := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT1_BU_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO GL_SEGMENT1_BU_TEMP
   SELECT *
   FROM (
      SELECT
         ffv.flex_value                                  BU_GL_SEGMENT1,
         fifs.id_flex_num                                CHART_OF_ACCOUNTS_ID,
         ffvt.DESCRIPTION                                BU_GL_DESCRIPTION,
         nvl(fr1.BU_GL_REGROUPEMENT_VENTE,'UNDEFINED')   BU_GL_REGROUPEMENT_VENTE,
         nvl(fr1.BU_GL_REGROUPEMENT_ACHAT,'UNDEFINED')   BU_GL_REGROUPEMENT_ACHAT,
         fr1.DATE_MAJ                                    DATE_MAJ_EXCEL,
         fr1.COMMENTAIRES                                COMMENTAIRES,
         ffv.CREATION_DATE                               CREATION_DATE   ,
         ffv.CREATED_BY                                  CREATED_BY,
         ffv.LAST_UPDATE_DATE                            LAST_UPDATE_DATE   ,
         ffv.LAST_UPDATED_BY                             LAST_UPDATED_BY,
         ROWNUM                                          ROW_NUMBER_ID,
         SYSDATE                                         ROW_CREATION_DATE,
         SYSDATE                                         ROW_LAST_UPDATE_DATE,
         fr1.BU_GL_PRIMETRE_SALESFORCE BU_GL_PERIMETRE_SALESFORCE
      FROM steph_apps_FND_ID_FLEX_SEGMENTS_VL_bz fifs
      INNER JOIN steph_apps_FND_FLEX_VALUES#_bz ffv
         ON ( fifs.flex_value_set_id  = ffv.flex_value_set_id )
      INNER JOIN  steph_apps_FND_FLEX_VALUES_tl_bz ffvt
         ON ( ffv.flex_value_id = ffvt.flex_value_id
         AND ffvt.language = 'US' )
      LEFT OUTER JOIN    file_bu_segment1_bz fr1
         ON ffv.flex_value  = fr1.bu_gl_code
      WHERE
         fifs.id_flex_num = 101
         AND fifs.id_flex_code = 'GL#'
         AND fifs.segment_num  = 1
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT1_BU_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO GL_SEGMENT1_BU_TEMP(
      BU_GL_SEGMENT1,
      CHART_OF_ACCOUNTS_ID,
      BU_GL_DESCRIPTION,
      BU_GL_REGROUPEMENT_VENTE,
      BU_GL_REGROUPEMENT_ACHAT,
      DATE_MAJ_EXCEL,
      COMMENTAIRES,
      CREATION_DATE   ,
      CREATED_BY,
      LAST_UPDATE_DATE   ,
      LAST_UPDATED_BY,
      ROW_NUMBER_ID,
      ROW_CREATION_DATE,
      ROW_LAST_UPDATE_DATE,
      BU_GL_PERIMETRE_SALESFORCE
   ) VALUES (
      '0000',
      101,
      'Undefined',
      'UNDEFINED',
      'UNDEFINED',
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      null,
      null,
      0  ,
      SYSDATE,
      SYSDATE,
      'N'
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT1_BU_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
   INSERT INTO GL_SEGMENT1_BU_TEMP(
      BU_GL_SEGMENT1,
      CHART_OF_ACCOUNTS_ID,
      BU_GL_DESCRIPTION,
      BU_GL_REGROUPEMENT_VENTE,
      BU_GL_REGROUPEMENT_ACHAT,
      DATE_MAJ_EXCEL,
      COMMENTAIRES,
      CREATION_DATE   ,
      CREATED_BY,
      LAST_UPDATE_DATE   ,
      LAST_UPDATED_BY,
      ROW_NUMBER_ID,
      ROW_CREATION_DATE,
      ROW_LAST_UPDATE_DATE,
      BU_GL_PERIMETRE_SALESFORCE
   ) VALUES (
      'KOREA',
      101,
      'LS-Korea',
      'MERGE',  --> modif le 06/08/25 avant 'LS'
      'ASIA',
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      null,
      null,
      9999999999  ,
      SYSDATE,
      SYSDATE,
      'N'
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT1_BU_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;

   INSERT INTO GL_SEGMENT1_BU_TEMP(
      BU_GL_SEGMENT1,
      CHART_OF_ACCOUNTS_ID,
      BU_GL_DESCRIPTION,
      BU_GL_REGROUPEMENT_VENTE,
      BU_GL_REGROUPEMENT_ACHAT,
      DATE_MAJ_EXCEL,
      COMMENTAIRES,
      CREATION_DATE   ,
      CREATED_BY,
      LAST_UPDATE_DATE   ,
      LAST_UPDATED_BY,
      ROW_NUMBER_ID,
      ROW_CREATION_DATE,
      ROW_LAST_UPDATE_DATE,
      BU_GL_PERIMETRE_SALESFORCE
      ) VALUES (
         'SINGAPORE',
         101,
         'LS-Singapore',
         'MERGE',  --> modif le 06/08/25 avant 'LS'
         'ASIA',
         TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
         null,
         TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
         null,
         null,
         null,
         9999999998  ,
         SYSDATE,
         SYSDATE,
         'N'
      );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT1_BU_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO GL_SEGMENT1_BU_TEMP(
      BU_GL_SEGMENT1,
      CHART_OF_ACCOUNTS_ID,
      BU_GL_DESCRIPTION,
      BU_GL_REGROUPEMENT_VENTE,
      BU_GL_REGROUPEMENT_ACHAT,
      DATE_MAJ_EXCEL,
      COMMENTAIRES,
      CREATION_DATE   ,
      CREATED_BY,
      LAST_UPDATE_DATE   ,
      LAST_UPDATED_BY,
      ROW_NUMBER_ID,
      ROW_CREATION_DATE,
      ROW_LAST_UPDATE_DATE,
      BU_GL_PERIMETRE_SALESFORCE
   ) VALUES (
      'AUSTRALIA',
      101,
      'LS-Australia',
      'MERGE',
      'ASIA',
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      null,
      null,
      9999999997  ,
      SYSDATE,
      SYSDATE,
      'N'
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT1_BU_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('GL_SEGMENT1_BU_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'   ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_Gl_Segment1_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_Gl_Segment2_Proc
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Gl_Segment2_Proc
IS
   v_procedure varchar2(100) := 'Recreate_Gl_Segment2_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT2_LOCATION_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
      EXECUTE IMMEDIATE 'TRUNCATE TABLE GL_SEGMENT2_LOCATION_TEMP';
   g_status    := 'COMPLETED';
   g_etape     := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT2_LOCATION_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO GL_SEGMENT2_LOCATION_TEMP
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
      FROM steph_apps_FND_ID_FLEX_SEGMENTS_VL_bz fifs
      INNER JOIN steph_apps_FND_FLEX_VALUES#_bz ffv
         ON ( fifs.flex_value_set_id = ffv.flex_value_set_id )
      INNER JOIN  steph_apps_FND_FLEX_VALUES_tl_bz ffvt
         ON ( ffv.flex_value_id = ffvt.flex_value_id
         AND ffvt.language = 'US' )
      LEFT OUTER JOIN file_location_segment2_bz fr2
         ON ffv.flex_value  = fr2.location_gl_code
      WHERE
         fifs.id_flex_num = 101
         AND fifs.id_flex_code = 'GL#'
         AND fifs.segment_num  = 2
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT2_LOCATION_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO GL_SEGMENT2_LOCATION_TEMP(
      LOCATION_GL_SEGMENT2,
      CHART_OF_ACCOUNTS_ID,
      LOCATION_GL_DESCRIPTION,
      LOCATION_GL_REGROUPEMENT_VENTE,
      LOCATION_GL_REGROUPEMENT_ACHAT,
      LOCATION_GL_BU_VENTE,
      CLE_BU_INVOICE,
      DATE_MAJ_EXCEL,
      COMMENTAIRES,
      CREATION_DATE   ,
      CREATED_BY,
      LAST_UPDATE_DATE   ,
      LAST_UPDATED_BY,
      ROW_NUMBER_ID,
      ROW_CREATION_DATE,
      ROW_LAST_UPDATE_DATE
   ) VALUES (
      'KOREA',
      101,
      'LS-Korea',
      'LOC SUBSIDIARY',
      'ASIA',
      'NOT AFFECTED',
      'segment_5',
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      null,
      null,
      9999999999  ,
      SYSDATE,
      SYSDATE
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT2_LOCATION_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO GL_SEGMENT2_LOCATION_TEMP(
      LOCATION_GL_SEGMENT2,
      CHART_OF_ACCOUNTS_ID,
      LOCATION_GL_DESCRIPTION,
      LOCATION_GL_REGROUPEMENT_VENTE,
      LOCATION_GL_REGROUPEMENT_ACHAT,
      LOCATION_GL_BU_VENTE,
      CLE_BU_INVOICE,
      DATE_MAJ_EXCEL,
      COMMENTAIRES,
      CREATION_DATE   ,
      CREATED_BY,
      LAST_UPDATE_DATE   ,
      LAST_UPDATED_BY,
      ROW_NUMBER_ID,
      ROW_CREATION_DATE,
      ROW_LAST_UPDATE_DATE
   ) VALUES (
      'SINGAPORE',
      101,
      'LS-Singapore',
      'LOC SUBSIDIARY',
      'ASIA',
      'NOT AFFECTED',
      'segment_5',
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      null,
      null,
      9999999998  ,
      SYSDATE,
      SYSDATE
   );
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT2_LOCATION_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
   INSERT INTO GL_SEGMENT2_LOCATION_TEMP(
      LOCATION_GL_SEGMENT2,
      CHART_OF_ACCOUNTS_ID,
      LOCATION_GL_DESCRIPTION,
      LOCATION_GL_REGROUPEMENT_VENTE,
      LOCATION_GL_REGROUPEMENT_ACHAT,
      LOCATION_GL_BU_VENTE,
      CLE_BU_INVOICE,
      DATE_MAJ_EXCEL,
      COMMENTAIRES,
      CREATION_DATE   ,
      CREATED_BY,
      LAST_UPDATE_DATE   ,
      LAST_UPDATED_BY,
      ROW_NUMBER_ID,
      ROW_CREATION_DATE,
      ROW_LAST_UPDATE_DATE
   ) VALUES (
      'AUSTALIA',
      101,
      'LS-Austalia',
      'LOC SUBSIDIARY',
      'ASIA',
      'NOT AFFECTED',
      'segment_5',
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
      null,
      null,
      null,
      9999999997,
      SYSDATE,
      SYSDATE
   );
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT2_LOCATION_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('GL_SEGMENT2_LOCATION_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'   ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_Gl_Segment2_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_Gl_Segment3_Proc
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Gl_Segment3_Proc
IS
   v_procedure varchar2(100) := 'Recreate_Gl_Segment3_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT3_DEPARTMENT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
      EXECUTE IMMEDIATE 'TRUNCATE TABLE GL_SEGMENT3_DEPARTMENT_TEMP';
   g_status    := 'COMPLETED';
   g_etape     := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT3_DEPARTMENT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO GL_SEGMENT3_DEPARTMENT_TEMP
   SELECT *
   FROM (
      SELECT
         ffv.flex_value                                        DEPARTMENT_GL_SEGMENT3,
         fifs.id_flex_num                                        CHART_OF_ACCOUNTS_ID,
         ffvt.DESCRIPTION                                        DEPARTMENT_GL_DESCRIPTION,
         nvl(fr3.DEPARTMENT_GL_REGROUPEMENT_VENTE,'UNDEFINED')   DEPARTMENT_GL_REGROUPEMENT_VENTE,
         nvl(fr3.DEPARTMENT_GL_REGROUPEMENT_ACHAT,'UNDEFINED')   DEPARTMENT_GL_REGROUPEMENT_ACHAT,
         nvl(fr3.DEPARTMENT_GL_BU_VENTE,'NOT AFFECTED')          DEPARTMENT_GL_BU_VENTE,
         fr3.DATE_MAJ                                            DATE_MAJ_EXCEL,
         fr3.COMMENTAIRES                                        COMMENTAIRES,
         ffv.CREATION_DATE                                       CREATION_DATE   ,
         ffv.CREATED_BY                                          CREATED_BY,
         ffv.LAST_UPDATE_DATE                                    LAST_UPDATE_DATE   ,
         ffv.LAST_UPDATED_BY                                     LAST_UPDATED_BY,
         ROWNUM                                                  ROW_NUMBER_ID,
         SYSDATE                                                 ROW_CREATION_DATE,
         SYSDATE                                                 ROW_LAST_UPDATE_DATE
      FROM steph_apps_FND_ID_FLEX_SEGMENTS_VL_bz fifs
      INNER JOIN steph_apps_FND_FLEX_VALUES#_bz ffv
         ON ( fifs.flex_value_set_id  = ffv.flex_value_set_id )
      INNER JOIN  steph_apps_FND_FLEX_VALUES_tl_bz ffvt
         ON ( ffv.flex_value_id = ffvt.flex_value_id
         AND ffvt.language = 'US' )
      LEFT OUTER JOIN file_department_segment3_bz fr3
         ON ffv.flex_value  = fr3.DEPARTMENT_gl_code
      WHERE 1=1
         AND fifs.id_flex_num = 101
         AND fifs.id_flex_code = 'GL#'
         AND fifs.segment_num  = 3
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT3_DEPARTMENT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('GL_SEGMENT3_DEPARTMENT_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'   ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'   ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_Gl_Segment3_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_Gl_Segment4_Proc
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Gl_Segment4_Proc
IS
   v_procedure varchar2(100) := 'Recreate_Gl_Segment4_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT4_NATURAL_ACCOUNT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
      EXECUTE IMMEDIATE 'TRUNCATE TABLE GL_SEGMENT4_NATURAL_ACCOUNT_TEMP';
   g_status    := 'COMPLETED';
   g_etape     := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT4_NATURAL_ACCOUNT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
/*  -- modification pour créer la hiérachie de groupe automatique
      INSERT INTO GL_SEGMENT4_NATURAL_ACCOUNT_TEMP
      SELECT *
      FROM (select  ffv.flex_value                                        NATURAL_ACCOUNT_GL_SEGMENT4,
                  fifs.id_flex_num                                        CHART_OF_ACCOUNTS_ID,
                  ffvt.DESCRIPTION                                        NATURAL_ACCOUNT_GL_DESCRIPTION,
                  SUBSTR(ffv.compiled_value_attributes,5,1)               NATURAL_ACCOUNT_GL_CATEGORIE,
                  nvl(fr4.NATURAL_ACCOUNT_GL_REGROUPEMENT_1,'UNDEFINED')  NATURAL_ACCOUNT_GL_REGROUPEMENT_1,
                  nvl(fr4.NATURAL_ACCOUNT_GL_REGROUPEMENT_2,'UNDEFINED')  NATURAL_ACCOUNT_GL_REGROUPEMENT_2,
                  nvl(fr4.NATURAL_ACCOUNT_GL_REGROUPEMENT_3,'UNDEFINED')  NATURAL_ACCOUNT_GL_REGROUPEMENT_3,
                  fr4.flag_item_ls_sales                                  FLAG_ITEM_LS_SALES,
                  fr4.DATE_MAJ                                            DATE_MAJ_EXCEL,
                  fr4.COMMENTAIRES                                        COMMENTAIRES,
                  ffv.CREATION_DATE                                       CREATION_DATE   ,
                  ffv.CREATED_BY                                          CREATED_BY,
                  ffv.LAST_UPDATE_DATE                                    LAST_UPDATE_DATE   ,
                  ffv.LAST_UPDATED_BY                                     LAST_UPDATED_BY,
                  ROWNUM                                                  ROW_NUMBER_ID,
                  SYSDATE                                                 ROW_CREATION_DATE,
                  SYSDATE                                                 ROW_LAST_UPDATE_DATE
            FROM steph_apps_FND_ID_FLEX_SEGMENTS_VL_bz fifs
            INNER JOIN steph_apps_FND_FLEX_VALUES#_bz ffv
               ON (    fifs.flex_value_set_id  = ffv.flex_value_set_id  )
            INNER JOIN  steph_apps_FND_FLEX_VALUES_tl_bz ffvt
               ON (    ffv.flex_value_id = ffvt.flex_value_id
                  and ffvt.language = 'US' )
         LEFT OUTER JOIN    file_naturalaccount_segment4_bz                 fr4
               ON ffv.flex_value  = fr4.natural_account_gl_code
            WHERE
                  fifs.id_flex_num  = 101
            and fifs.id_flex_code = 'GL#'
            and fifs.segment_num  = 4
         );
*/
   INSERT INTO GL_SEGMENT4_NATURAL_ACCOUNT_TEMP
   SELECT *
   FROM (
      WITH
      creation_group_temp AS (
         SELECT
            ffv.flex_value AS NATURAL_ACCOUNT_GL_SEGMENT4,
            NVL(fr4.NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_1, 'UNDEFINED') AS NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_1,
            NVL(fr4.NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_2, 'UNDEFINED') AS NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_2,
            NVL(fr4.NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_3, 'UNDEFINED') AS NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_3,
            fr4.flag_item_ls_sales AS FLAG_ITEM_LS_SALES,
            fr4.DATE_MAJ_EXCEL,
            fr4.COMMENTAIRES AS COMMENTAIRES,
            fifs.id_flex_num AS CHART_OF_ACCOUNTS_ID,
            ffvt.DESCRIPTION AS NATURAL_ACCOUNT_GL_DESCRIPTION,
            SUBSTR(ffv.compiled_value_attributes, 5, 1) AS NATURAL_ACCOUNT_GL_CATEGORIE,
            (
               SELECT ffvt1.DESCRIPTION
               FROM steph_apps_FND_FLEX_VALUES_tl_bz ffvt1
               WHERE
                  ffvt1.flex_value_id = (
                     SELECT ffv2.flex_value_id
                     FROM steph_apps_FND_FLEX_VALUES#_bz ffv2
                     WHERE
                        ffv2.flex_value_set_id = ffv.flex_value_set_id
                        AND ffv2.flex_value = SUBSTR(ffv.flex_value, 1, 1) || 'ZZZZZZZ'
                  )
                  AND ROWNUM = 1
                  AND ffvt1.language = 'US'
            ) AS GROUP1,
            (
               SELECT ffvt1.DESCRIPTION
               FROM steph_apps_FND_FLEX_VALUES_tl_bz ffvt1
               WHERE
                  ffvt1.flex_value_id = (
                     SELECT ffv2.flex_value_id
                     FROM steph_apps_FND_FLEX_VALUES#_bz ffv2
                     WHERE
                        ffv2.flex_value_set_id = ffv.flex_value_set_id
                        AND ffv2.flex_value = SUBSTR(ffv.flex_value, 1, 2) || 'ZZZZZZ'
                  )
                  AND ROWNUM = 1
                  AND ffvt1.language = 'US'
            ) AS GROUP2,
            (
               SELECT ffvt1.DESCRIPTION
               FROM steph_apps_FND_FLEX_VALUES_tl_bz ffvt1
               WHERE
                  ffvt1.flex_value_id = (
                     SELECT ffv2.flex_value_id
                     FROM steph_apps_FND_FLEX_VALUES#_bz ffv2
                     WHERE
                        ffv2.flex_value_set_id = ffv.flex_value_set_id
                        AND ffv2.flex_value = SUBSTR(ffv.flex_value, 1, 3) || 'ZZZZZ'
                  )
                  AND ROWNUM = 1
                  AND ffvt1.language = 'US'
            ) AS GROUP3,
            (
               SELECT ffvt1.DESCRIPTION
               FROM steph_apps_FND_FLEX_VALUES_tl_bz ffvt1
               WHERE
                  ffvt1.flex_value_id = (
                     SELECT ffv2.flex_value_id
                     FROM steph_apps_FND_FLEX_VALUES#_bz ffv2
                     WHERE
                        ffv2.flex_value_set_id = ffv.flex_value_set_id
                        AND ffv2.flex_value = SUBSTR(ffv.flex_value, 1, 4) || 'ZZZZ'
                  )
                  AND ROWNUM = 1
                  AND ffvt1.language = 'US'
            ) AS GROUP4,
            (
               SELECT ffvt1.DESCRIPTION
               FROM steph_apps_FND_FLEX_VALUES_tl_bz ffvt1
               WHERE
                  ffvt1.flex_value_id = (
                     SELECT ffv2.flex_value_id
                     FROM steph_apps_FND_FLEX_VALUES#_bz ffv2
                     WHERE
                        ffv2.flex_value_set_id = ffv.flex_value_set_id
                        AND ffv2.flex_value = SUBSTR(ffv.flex_value, 1, 5) || 'ZZZ'
                  )
                  AND ROWNUM = 1
                  AND ffvt1.language = 'US'
            ) AS GROUP5,
            (
               SELECT ffvt1.DESCRIPTION
               FROM steph_apps_FND_FLEX_VALUES_tl_bz ffvt1
               WHERE
                  ffvt1.flex_value_id = (
                     SELECT ffv2.flex_value_id
                     FROM steph_apps_FND_FLEX_VALUES#_bz ffv2
                     WHERE
                        ffv2.flex_value_set_id = ffv.flex_value_set_id
                        AND ffv2.flex_value = SUBSTR(ffv.flex_value, 1, 6) || 'ZZ'
                  )
                  AND ROWNUM = 1
                  AND ffvt1.language = 'US'
            ) AS GROUP6,
            (
               SELECT ffvt1.DESCRIPTION
               FROM steph_apps_FND_FLEX_VALUES_tl_bz ffvt1
               WHERE
                  ffvt1.flex_value_id = (
                     SELECT ffv2.flex_value_id
                     FROM steph_apps_FND_FLEX_VALUES#_bz ffv2
                     WHERE
                        ffv2.flex_value_set_id = ffv.flex_value_set_id
                        AND ffv2.flex_value = SUBSTR(ffv.flex_value, 1, 7) || 'Z'
                  )
                  AND ffvt1.language = 'US'
                  AND ROWNUM = 1
            ) AS GROUP7,
            ffv.CREATION_DATE,
            ffv.CREATED_BY,
            ffv.LAST_UPDATE_DATE,
            ffv.LAST_UPDATED_BY,
            ROWNUM AS ROW_NUMBER_ID,
            SYSDATE AS ROW_CREATION_DATE,
            SYSDATE AS ROW_LAST_UPDATE_DATE
         FROM steph_apps_FND_ID_FLEX_SEGMENTS_VL_bz fifs
         INNER JOIN steph_apps_FND_FLEX_VALUES#_bz ffv
            ON ( fifs.flex_value_set_id  = ffv.flex_value_set_id )
         INNER JOIN steph_apps_FND_FLEX_VALUES_tl_bz ffvt
            ON ( ffv.flex_value_id = ffvt.flex_value_id
            AND ffvt.language = 'US' )
         LEFT OUTER JOIN file_naturalaccount_segment4_bz fr4
            ON ffv.flex_value = fr4.natural_account_gl_code
         WHERE
            fifs.id_flex_num = 101
            AND fifs.id_flex_code = 'GL#'
            AND fifs.segment_num  = 4
      ),
      count_non_null_group AS (
         SELECT
            NATURAL_ACCOUNT_GL_SEGMENT4,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_1,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_2,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_3,
            FLAG_ITEM_LS_SALES,
            DATE_MAJ_EXCEL,
            COMMENTAIRES,
            CHART_OF_ACCOUNTS_ID,
            NATURAL_ACCOUNT_GL_DESCRIPTION,
            NATURAL_ACCOUNT_GL_CATEGORIE,
            GROUP1,
            GROUP2,
            GROUP3,
            GROUP4,
            GROUP5,
            GROUP6,
            GROUP7,
            CREATION_DATE,
            CREATED_BY,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY,
            ROW_NUMBER_ID,
            ROW_CREATION_DATE,
            ROW_LAST_UPDATE_DATE,
            (CASE WHEN group1 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN group2 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN group3 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN group4 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN group5 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN group6 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN group7 IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count,
            (CASE WHEN group1 IS NOT NULL THEN 'group1 ' ELSE '' END ||
            CASE WHEN group2 IS NOT NULL THEN 'group2 ' ELSE '' END ||
            CASE WHEN group3 IS NOT NULL THEN 'group3 ' ELSE '' END ||
            CASE WHEN group4 IS NOT NULL THEN 'group4 ' ELSE '' END ||
            CASE WHEN group5 IS NOT NULL THEN 'group5 ' ELSE '' END ||
            CASE WHEN group6 IS NOT NULL THEN 'group6 ' ELSE '' END ||
            CASE WHEN group7 IS NOT NULL THEN 'group7 ' ELSE '' END) AS non_null_groups
         FROM creation_group_temp
      )
      SELECT
         NATURAL_ACCOUNT_GL_SEGMENT4,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_1,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_2,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_3,
         FLAG_ITEM_LS_SALES,
         DATE_MAJ_EXCEL,
         COMMENTAIRES,
         CHART_OF_ACCOUNTS_ID,
         NATURAL_ACCOUNT_GL_DESCRIPTION,
         NATURAL_ACCOUNT_GL_CATEGORIE,
         -- non_null_count,

         -- Exemple avec GROUP2
         -- 1. INSTR(non_null_groups, ' ') + 1 : Trouve la position du premier espace et ajoute 1 pour obtenir le début du deuxième mot (-1 pour le premiers).
         -- 2. INSTR(non_null_groups, ' ', 1, 2) : Trouve la position du deuxième espace.
         -- 3. SUBSTR(non_null_groups, INSTR(non_null_groups, ' ') + 1, INSTR(non_null_groups, ' ', 1, 2) - INSTR(non_null_groups, ' ') - 1) :
         --         Extrait le sous-ensemble de la chaîne 'non_null_groups' qui correspond au deuxième mot.
         -- 4. = 'group1' : Compare le deuxième mot extrait avec la chaîne 'group1'.
         --  Si le deuxième mot est 'group1', alors la condition retourne GROUP1.

      /* COALESCE(group1, group2, group3, group4, group5, group6, group7) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_1,
         COALESCE(group2, group3, group4, group5, group6, group7) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_2,
         COALESCE(group3, group4, group5, group6, group7) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_3,
         COALESCE(group4, group5, group6, group7) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_4,
         COALESCE(group5, group6, group7) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_5,
         COALESCE(group6, group7) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_6,
         group7 as NATURAL_ACCOUNT_GL_REGROUPEMENT_7,  */

         COALESCE(group1, group2, group3, group4, group5, group6, group7) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_1,
         COALESCE(group2, group3, group4, group5, group6, group7, group1) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_2,
         COALESCE(group3, group4, group5, group6, group7, group2, group1) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_3,
         COALESCE(group4, group5, group6, group7, group3, group2, group1) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_4,
         COALESCE(group5, group6, group7, group4, group3, group2, group1) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_5,
         COALESCE(group6, group7, group5, group4, group3, group2, group1) AS NATURAL_ACCOUNT_GL_REGROUPEMENT_6,
         COALESCE(group7, group6, group5, group4, group3, group2, group1) as NATURAL_ACCOUNT_GL_REGROUPEMENT_7,

      /* CASE WHEN non_null_count >= 1 THEN
            CASE
               WHEN SUBSTR(non_null_groups, 1, INSTR(non_null_groups, ' ') - 1) = 'group1' THEN GROUP1
               WHEN SUBSTR(non_null_groups, 1, INSTR(non_null_groups, ' ') - 1) = 'group2' THEN GROUP2
               WHEN SUBSTR(non_null_groups, 1, INSTR(non_null_groups, ' ') - 1) = 'group3' THEN GROUP3
               WHEN SUBSTR(non_null_groups, 1, INSTR(non_null_groups, ' ') - 1) = 'group4' THEN GROUP4
               WHEN SUBSTR(non_null_groups, 1, INSTR(non_null_groups, ' ') - 1) = 'group5' THEN GROUP5
               WHEN SUBSTR(non_null_groups, 1, INSTR(non_null_groups, ' ') - 1) = 'group6' THEN GROUP6
               WHEN SUBSTR(non_null_groups, 1, INSTR(non_null_groups, ' ') - 1) = 'group7' THEN GROUP7
            END
         ELSE NULL END AS NATURAL_ACCOUNT_GL_REGROUPEMENT_1,
         CASE WHEN non_null_count >= 2 THEN
            CASE
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ') + 1, INSTR(non_null_groups, ' ', 1, 2) - INSTR(non_null_groups, ' ') - 1) = 'group1' THEN GROUP1
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ') + 1, INSTR(non_null_groups, ' ', 1, 2) - INSTR(non_null_groups, ' ') - 1) = 'group2' THEN GROUP2
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ') + 1, INSTR(non_null_groups, ' ', 1, 2) - INSTR(non_null_groups, ' ') - 1) = 'group3' THEN GROUP3
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ') + 1, INSTR(non_null_groups, ' ', 1, 2) - INSTR(non_null_groups, ' ') - 1) = 'group4' THEN GROUP4
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ') + 1, INSTR(non_null_groups, ' ', 1, 2) - INSTR(non_null_groups, ' ') - 1) = 'group5' THEN GROUP5
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ') + 1, INSTR(non_null_groups, ' ', 1, 2) - INSTR(non_null_groups, ' ') - 1) = 'group6' THEN GROUP6
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ') + 1, INSTR(non_null_groups, ' ', 1, 2) - INSTR(non_null_groups, ' ') - 1) = 'group7' THEN GROUP7
            END
         ELSE NULL END AS NATURAL_ACCOUNT_GL_REGROUPEMENT_2,
         CASE WHEN non_null_count >= 3 THEN
            CASE
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 2) + 1, INSTR(non_null_groups, ' ', 1, 3) - INSTR(non_null_groups, ' ', 1, 2) - 1) = 'group1' THEN GROUP1
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 2) + 1, INSTR(non_null_groups, ' ', 1, 3) - INSTR(non_null_groups, ' ', 1, 2) - 1) = 'group2' THEN GROUP2
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 2) + 1, INSTR(non_null_groups, ' ', 1, 3) - INSTR(non_null_groups, ' ', 1, 2) - 1) = 'group3' THEN GROUP3
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 2) + 1, INSTR(non_null_groups, ' ', 1, 3) - INSTR(non_null_groups, ' ', 1, 2) - 1) = 'group4' THEN GROUP4
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 2) + 1, INSTR(non_null_groups, ' ', 1, 3) - INSTR(non_null_groups, ' ', 1, 2) - 1) = 'group5' THEN GROUP5
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 2) + 1, INSTR(non_null_groups, ' ', 1, 3) - INSTR(non_null_groups, ' ', 1, 2) - 1) = 'group6' THEN GROUP6
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 2) + 1, INSTR(non_null_groups, ' ', 1, 3) - INSTR(non_null_groups, ' ', 1, 2) - 1) = 'group7' THEN GROUP7
            END
         ELSE NULL END AS NATURAL_ACCOUNT_GL_REGROUPEMENT_3,
         CASE WHEN non_null_count >= 4 THEN
            CASE
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 3) + 1, INSTR(non_null_groups, ' ', 1, 4) - INSTR(non_null_groups, ' ', 1, 3) - 1) = 'group1' THEN GROUP1
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 3) + 1, INSTR(non_null_groups, ' ', 1, 4) - INSTR(non_null_groups, ' ', 1, 3) - 1) = 'group2' THEN GROUP2
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 3) + 1, INSTR(non_null_groups, ' ', 1, 4) - INSTR(non_null_groups, ' ', 1, 3) - 1) = 'group3' THEN GROUP3
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 3) + 1, INSTR(non_null_groups, ' ', 1, 4) - INSTR(non_null_groups, ' ', 1, 3) - 1) = 'group4' THEN GROUP4
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 3) + 1, INSTR(non_null_groups, ' ', 1, 4) - INSTR(non_null_groups, ' ', 1, 3) - 1) = 'group5' THEN GROUP5
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 3) + 1, INSTR(non_null_groups, ' ', 1, 4) - INSTR(non_null_groups, ' ', 1, 3) - 1) = 'group6' THEN GROUP6
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 3) + 1, INSTR(non_null_groups, ' ', 1, 4) - INSTR(non_null_groups, ' ', 1, 3) - 1) = 'group7' THEN GROUP7
            END
         ELSE NULL END AS NATURAL_ACCOUNT_GL_REGROUPEMENT_4,
         CASE WHEN non_null_count >= 5 THEN
            CASE
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 4) + 1, INSTR(non_null_groups, ' ', 1, 5) - INSTR(non_null_groups, ' ', 1, 4) - 1) = 'group1' THEN GROUP1
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 4) + 1, INSTR(non_null_groups, ' ', 1, 5) - INSTR(non_null_groups, ' ', 1, 4) - 1) = 'group2' THEN GROUP2
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 4) + 1, INSTR(non_null_groups, ' ', 1, 5) - INSTR(non_null_groups, ' ', 1, 4) - 1) = 'group3' THEN GROUP3
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 4) + 1, INSTR(non_null_groups, ' ', 1, 5) - INSTR(non_null_groups, ' ', 1, 4) - 1) = 'group4' THEN GROUP4
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 4) + 1, INSTR(non_null_groups, ' ', 1, 5) - INSTR(non_null_groups, ' ', 1, 4) - 1) = 'group5' THEN GROUP5
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 4) + 1, INSTR(non_null_groups, ' ', 1, 5) - INSTR(non_null_groups, ' ', 1, 4) - 1) = 'group6' THEN GROUP6
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 4) + 1, INSTR(non_null_groups, ' ', 1, 5) - INSTR(non_null_groups, ' ', 1, 4) - 1) = 'group7' THEN GROUP7
            END
         ELSE NULL END AS NATURAL_ACCOUNT_GL_REGROUPEMENT_5,
         CASE WHEN non_null_count >= 6 THEN
            CASE
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 5) + 1, INSTR(non_null_groups, ' ', 1, 6) - INSTR(non_null_groups, ' ', 1, 5) - 1) = 'group1' THEN GROUP1
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 5) + 1, INSTR(non_null_groups, ' ', 1, 6) - INSTR(non_null_groups, ' ', 1, 5) - 1) = 'group2' THEN GROUP2
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 5) + 1, INSTR(non_null_groups, ' ', 1, 6) - INSTR(non_null_groups, ' ', 1, 5) - 1) = 'group3' THEN GROUP3
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 5) + 1, INSTR(non_null_groups, ' ', 1, 6) - INSTR(non_null_groups, ' ', 1, 5) - 1) = 'group4' THEN GROUP4
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 5) + 1, INSTR(non_null_groups, ' ', 1, 6) - INSTR(non_null_groups, ' ', 1, 5) - 1) = 'group5' THEN GROUP5
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 5) + 1, INSTR(non_null_groups, ' ', 1, 6) - INSTR(non_null_groups, ' ', 1, 5) - 1) = 'group6' THEN GROUP6
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 5) + 1, INSTR(non_null_groups, ' ', 1, 6) - INSTR(non_null_groups, ' ', 1, 5) - 1) = 'group7' THEN GROUP7
            END
         ELSE NULL END AS NATURAL_ACCOUNT_GL_REGROUPEMENT_6,
         CASE WHEN non_null_count >= 7 THEN
            CASE
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 6) + 1) = 'group1' THEN GROUP1
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 6) + 1) = 'group2' THEN GROUP2
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 6) + 1) = 'group3' THEN GROUP3
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 6) + 1) = 'group4' THEN GROUP4
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 6) + 1) = 'group5' THEN GROUP5
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 6) + 1) = 'group6' THEN GROUP6
               WHEN SUBSTR(non_null_groups, INSTR(non_null_groups, ' ', 1, 6) + 1) = 'group7' THEN GROUP7
            END
         ELSE NULL END AS NATURAL_ACCOUNT_GL_REGROUPEMENT_7 ,*/  --a voir si utile car toujours NULL sauf pour 32 Segments pour non_null_count=7 et 14 segments pour non_null_count=6 tous étant des cas généraux (1ZZZZZZZ,11ZZZZZZ,12XZZZZZ,12ZZZZZZ etc ....)
         CREATION_DATE,
         CREATED_BY,
         LAST_UPDATE_DATE,
         LAST_UPDATED_BY,
         ROW_NUMBER_ID,
         ROW_CREATION_DATE,
         ROW_LAST_UPDATE_DATE
      FROM count_non_null_group
      -- WHERE REGEXP_LIKE(natural_account_gl_segment4, '^[0-9]+$') pour supprimer les cas généreaux de la liste (1ZZZZZZZ,11ZZZZZZ,12XZZZZZ,12ZZZZZZ etc ....)
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

/* en commentaire de POTC le 16/06/25 car il sera créé dans l'INSERT suivant
   g_table     := 'GL_SEGMENT4_NATURAL_ACCOUNT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      INSERT INTO GL_SEGMENT4_NATURAL_ACCOUNT_TEMP(
            NATURAL_ACCOUNT_GL_SEGMENT4,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_1,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_2,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_3,
            FLAG_ITEM_LS_SALES,
            DATE_MAJ_EXCEL,
            COMMENTAIRES,
            CHART_OF_ACCOUNTS_ID,
            NATURAL_ACCOUNT_GL_DESCRIPTION,
            NATURAL_ACCOUNT_GL_CATEGORIE,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_1,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_2,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_3,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_4,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_5,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_6,
            NATURAL_ACCOUNT_GL_REGROUPEMENT_7,
            CREATION_DATE,
            CREATED_BY,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY,
            ROW_NUMBER_ID,
            ROW_CREATION_DATE,
            ROW_LAST_UPDATE_DATE
         )
      VALUES (
                  '00000000',
                  'UNDEFINED',
                  'UNDEFINED',
                  'UNDEFINED',
                  null,
                  null,
                  null,
                  101,
                  'Undefined',
                  null,
                  'Undefined',
                  'Undefined',
                  'Undefined',
                  'Undefined',
                  'Undefined',
                  'Undefined',
                  'Undefined',
                  TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
                  null,
                  TO_TIMESTAMP('01/01/52 00:00:00', 'DD/MM/YY HH24:MI:SS.FF9'),
                  null,
                  0  ,
                  SYSDATE,
                  SYSDATE
         );
   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;
*/

   g_table     := 'GL_SEGMENT4_NATURAL_ACCOUNT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO GL_SEGMENT4_NATURAL_ACCOUNT_TEMP
   SELECT *
   FROM (
      SELECT
         NATURAL_ACCOUNT_GL_CODE NATURAL_ACCOUNT_GL_SEGMENT4,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_1,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_2,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_ACHAT_3,
         FLAG_ITEM_LS_SALES,
         DATE_MAJ_EXCEL,
         COMMENTAIRES,
         CHART_OF_ACCOUNTS_ID,
         NATURAL_ACCOUNT_GL_DESCRIPTION,
         NATURAL_ACCOUNT_GL_CATEGORIE,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_1,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_2,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_3,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_4,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_5,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_6,
         NATURAL_ACCOUNT_GL_REGROUPEMENT_7,
         CREATION_DATE,
         CREATED_BY,
         LAST_UPDATE_DATE,
         LAST_UPDATED_BY,
         0 ROW_NUMBER_ID,
         sysdate ROW_CREATION_DATE,
         sysdate ROW_LAST_UPDATE_DATE
      FROM file_naturalaccount_segment4_bz
      WHERE NATURAL_ACCOUNT_GL_CATEGORIE = 'NOT ORACLE'
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT4_NATURAL_ACCOUNT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('GL_SEGMENT4_NATURAL_ACCOUNT_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'   ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_Gl_Segment4_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_Gl_Segment5_Proc
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Gl_Segment5_Proc
IS
   v_procedure varchar2(100) := 'Recreate_Gl_Segment5_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT5_PRODUCT_GROUP_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE GL_SEGMENT5_PRODUCT_GROUP_TEMP';
   g_status    := 'COMPLETED';
   g_etape     := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT5_PRODUCT_GROUP_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO GL_SEGMENT5_PRODUCT_GROUP_TEMP
   SELECT *
   FROM (
      SELECT
         ffv.flex_value                                              PRODUCT_GROUP_GL_SEGMENT5,
         fifs.id_flex_num                                            CHART_OF_ACCOUNTS_ID,
         ffvt.DESCRIPTION                                            PRODUCT_GROUP_GL_DESCRIPTION,
         nvl(fr5.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE,'NOT AFFECTED') PRODUCT_GROUP_GL_REGROUPEMENT_VENTE,
         nvl(fr5.PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT,'NOT AFFECTED') PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT,
         nvl(fr5.PRODUCT_GROUP_GL_BU_VENTE,'NOT AFFECTED')           PRODUCT_GROUP_GL_BU_VENTE,
         fr5.DATE_MAJ                                                DATE_MAJ_EXCEL,
         fr5.COMMENTAIRES                                            COMMENTAIRES,
         ffv.CREATION_DATE                                           CREATION_DATE,
         ffv.CREATED_BY                                              CREATED_BY,
         ffv.LAST_UPDATE_DATE                                        LAST_UPDATE_DATE,
         ffv.LAST_UPDATED_BY                                         LAST_UPDATED_BY,
         ROWNUM                                                      ROW_NUMBER_ID,
         SYSDATE                                                     ROW_CREATION_DATE,
         SYSDATE                                                     ROW_LAST_UPDATE_DATE
      FROM steph_apps_FND_ID_FLEX_SEGMENTS_VL_bz fifs
      INNER JOIN steph_apps_FND_FLEX_VALUES#_bz ffv
         ON ( fifs.flex_value_set_id = ffv.flex_value_set_id )
      INNER JOIN  steph_apps_FND_FLEX_VALUES_tl_bz ffvt
         ON ( ffv.flex_value_id = ffvt.flex_value_id
         AND ffvt.language = 'US' )
      LEFT OUTER JOIN    file_productgroup_segment5_bz fr5
         ON ffv.flex_value  = fr5.PRODUCT_GROUP_gl_code
      WHERE
         fifs.id_flex_num = 101
         AND fifs.id_flex_code = 'GL#'
         AND fifs.segment_num  = 5
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT5_PRODUCT_GROUP_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('GL_SEGMENT5_PRODUCT_GROUP_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_Gl_Segment5_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_Gl_Segment6_Proc
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Gl_Segment6_Proc
IS
   v_procedure varchar2(100) := 'Recreate_Gl_Segment6_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT6_INTERCOMPANY_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
      EXECUTE IMMEDIATE 'TRUNCATE TABLE GL_SEGMENT6_INTERCOMPANY_TEMP';
   g_status    := 'COMPLETED';
   g_etape     := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT6_INTERCOMPANY_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO GL_SEGMENT6_INTERCOMPANY_TEMP
   SELECT *
   FROM (
      SELECT
         ffv.flex_value                                           INTERCOMPANY_GL_SEGMENT6,
         fifs.id_flex_num                                         CHART_OF_ACCOUNTS_ID,
         ffvt.DESCRIPTION                                         INTERCOMPANY_GL_DESCRIPTION,
         nvl(fr6.INTERCOMPANY_TYPE,'UNDEFINED')                   INTERCOMPANY_TYPE,
         nvl(fr6.PERIMETRE_LS_CONSO,'Y')                          PERIMETRE_LS_CONSO,
         nvl(fr6.INTERCOMPANY_GL_REGROUPEMENT_ACHAT,'UNDEFINED')  INTERCOMPANY_GL_REGROUPEMENT_ACHAT,
         fr6.DATE_MAJ                                             DATE_MAJ_EXCEL,
         fr6.COMMENTAIRES                                         COMMENTAIRES,
         ffv.CREATION_DATE                                        CREATION_DATE,
         ffv.CREATED_BY                                           CREATED_BY,
         ffv.LAST_UPDATE_DATE                                     LAST_UPDATE_DATE,
         ffv.LAST_UPDATED_BY                                      LAST_UPDATED_BY,
         ROWNUM                                                   ROW_NUMBER_ID,
         SYSDATE                                                  ROW_CREATION_DATE,
         SYSDATE                                                  ROW_LAST_UPDATE_DATE
      FROM steph_apps_FND_ID_FLEX_SEGMENTS_VL_bz fifs
      INNER JOIN steph_apps_FND_FLEX_VALUES#_bz ffv
         ON ( fifs.flex_value_set_id = ffv.flex_value_set_id )
      INNER JOIN  steph_apps_FND_FLEX_VALUES_tl_bz ffvt
         ON ( ffv.flex_value_id = ffvt.flex_value_id
         AND ffvt.language = 'US' )
      LEFT OUTER JOIN file_INTERCO_segment6_bz fr6
         ON ffv.flex_value = fr6.INTERCOMPANY_gl_code
      WHERE
         fifs.id_flex_num  = 101
         AND fifs.id_flex_code = 'GL#'
         AND fifs.segment_num  = 6
   );

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'GL_SEGMENT6_INTERCOMPANY_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('GL_SEGMENT6_INTERCOMPANY_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_Gl_Segment6_Proc;

/****************************************************************************************
* PROCEDURE   :  Daily_conversion_rates_proc
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE DAILY_CONVERSION_RATES_PROC
IS
   v_procedure varchar2(100) := 'DAILY_CONVERSION_RATES_PROC';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;


   g_table     := 'DAILY_CONVERSION_RATES';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE DAILY_CONVERSION_RATES';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;


   g_table     := 'DAILY_CONVERSION_RATES';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;

   INSERT INTO DAILY_CONVERSION_RATES (
      FROM_CURRENCY,
      TO_CURRENCY,
      CONVERSION_DATE,
      CONVERSION_RATE,
      CONVERSION_TYPE,
      ROW_NUMBER_ID,
      ROW_CREATION_DATE,
      ROW_LAST_UPDATE_DATE
   )
   SELECT
      sagd.FROM_CURRENCY FROM_CURRENCY,
      sagd.TO_CURRENCY TO_CURRENCY,
      sagd.CONVERSION_DATE CONVERSION_DATE,
      sagd.CONVERSION_RATE CONVERSION_RATE,
      sagd.CONVERSION_TYPE CONVERSION_TYPE,
      ROWNUM ROW_NUMBER_ID,
      SYSDATE ROW_CREATION_DATE,
      SYSDATE ROW_LAST_UPDATE_DATE
   FROM STEPH_APPS_GL_DAILY_RATES_BZ sagd
   ;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   -- g_etape := '100';
   g_table     := 'DAILY_CONVERSION_RATES';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('DAILY_CONVERSION_RATES');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'   ;
         ROLLBACK;
      g_status   := 'COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END DAILY_CONVERSION_RATES_PROC;

/****************************************************************************************
* PROCEDURE   :  Fixed_rate_proc
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE FIXED_RATE_PROC
IS
   v_procedure varchar2(100) := 'Fixed_rate_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN

   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    :='BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'FIXED_RATE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE FIXED_RATE'  ;
   g_status    := 'COMPLETED';
   g_etape     := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'FIXED_RATE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;

   INSERT INTO FIXED_RATE (
      FROM_CURRENCY,
      TO_CURRENCY,
      FIXED_RATE_DIV,
      FIXED_RATE_MULT,
      FETCH_DATE
   )
   SELECT DISTINCT
      ffr.FROM_CURRENCY "FROM_CURRENCY",
      ffr.TO_CURRENCY "TO_CURRENCY",
      ffr.FIXED_RATE_DIV "FIXED_RATE_DIV",
      ffr.FIXED_RATE_MULT "FIXED_RATE_MULT",
      ffr.FETCH_DATE "FETCH_DATE"
   FROM
      FILE_FIXED_RATE_bz ffr
   ;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := 'FIXED_RATE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('FIXED_RATE');
   g_status   := 'COMPLETED';
   g_etape    := '022 - STATS' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   :='COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   :='END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END FIXED_RATE_PROC;

/****************************************************************************************
* PROCEDURE   :  Recreate_Country_Zone_Proc
* DESCRIPTION :  Create table country_zone
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Country_Zone_Proc IS
      v_procedure varchar2(100) := 'Recreate_Country_Zone_Proc';
      v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

--  g_etape := '101';
   g_table     := 'COUNTRY_ZONE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE COUNTRY_ZONE';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'COUNTRY_ZONE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO COUNTRY_ZONE
   SELECT
      fcz.COUNTRY_CODE,
      fcz.ZONE_1_CIMD,
      fcz.ZONE_2_CIMD,
      fcz.ZONE_3_CIMD,
      fcz.ZONE_EPG,
      fcz.ZONE_MDE,
      fcz.COUNTRY_NAME_EN,
      fcz.COUNTRY_NAME_FR,
      fcz.COUNTRY_ABREVIATION,
      fcz.COUNTRY_OTHER_NAME_1,
      fcz.COUNTRY_OTHER_NAME_2,
      fcz.COUNTRY_OTHER_NAME_3,
      case
         when salv.lookup_type = 'PER_EU_COUNTRIES' then 'Y'
         else 'N'
      end as PER_EU_COUNTRIES,
      ROWNUM ROW_NUMBER_ID,
      SYSDATE ROW_CREATION_DATE,
      SYSDATE ROW_LAST_UPDATE_DATE,
      fcz.CURRENCY ,
      fcz.DATE_MAJ,
      fcz.COMMENTAIRES
   FROM FILE_COUNTRY_ZONE_BZ fcz
   LEFT JOIN STEPH_APPS_FND_LOOKUP_VALUES_BZ salv
      ON salv.lookup_code = fcz.country_code
      AND salv.lookup_type = 'PER_EU_COUNTRIES'
      AND language = 'US';

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   -- g_etape := '100';
   g_table     := 'COUNTRY_ZONE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('COUNTRY_ZONE');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
   Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;
END Recreate_Country_Zone_Proc;

/****************************************************************************************
* PROCEDURE   :  Recreate_Customer_Segement_Proc
* DESCRIPTION :  Create table VAR_CUSTOMERS_SEGMENT_BU
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Recreate_Customer_Segement_Proc
IS
   v_procedure varchar2(100) := 'Recreate_Customer_Segement_Proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'VAR_CUSTOMERS_SEGMENT_BU';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE VAR_CUSTOMERS_SEGMENT_BU';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'VAR_CUSTOMERS_SEGMENT_BU';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO VAR_CUSTOMERS_SEGMENT_BU
   SELECT
      vcsb.ACCOUNT_NUMBER ,
      vcsb.BU,
      vcsb.CUSTOMER_SEGMENT,
      ROWNUM ROW_NUMBER_ID,
      SYSDATE ROW_CREATION_DATE,
      SYSDATE ROW_LAST_UPDATE_DATE
   FROM file_var_CUSTOMERS_SEGMENT_bu_bz vcsb ;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'VAR_CUSTOMERS_SEGMENT_BU';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('VAR_CUSTOMERS_SEGMENT_BU');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
   Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END Recreate_Customer_Segement_Proc;

/****************************************************************************************
* PROCEDURE   :  COMMANDE_HORS_BACKLOG_PROC
* DESCRIPTION :  Create table COMMANDE_HORS_BACKLOG
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE COMMANDE_HORS_BACKLOG_PROC
IS
   v_procedure varchar2(100) := 'COMMANDE_HORS_BACKLOG_PROC';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'COMMANDE_HORS_BACKLOG';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE COMMANDE_HORS_BACKLOG';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'COMMANDE_HORS_BACKLOG';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO COMMANDE_HORS_BACKLOG
      WITH vw AS (
         SELECT DISTINCT
            OU_NAME OU_NAME,
            to_number(CDE) CDE,
            LGN_CDE LGN_CDE,
            to_number(SUBSTR(LGN_CDE, 1, 1)) AS line_number,
            to_number(SUBSTR(LGN_CDE, -1, 1)) AS shipment_number,
            -- DEMANDE_PAR DEMANDE_PAR,                      -- mis en commentaire pour ne pas avoir de doublon , POTC le 06/02/25
            -- DATE_DEMANDE DATE_DEMANDE,                    -- mis en commentaire pour ne pas avoir de doublon , POTC le 06/02/25
            -- "  NO_INCIDENT" NO_INCIDENT,                  -- mis en commentaire pour ne pas avoir de doublon , POTC le 06/02/25
            -- CDE_REMPLACEMENT CDE_REMPLACEMENT,            -- mis en commentaire pour ne pas avoir de doublon , POTC le 06/02/25
            -- COMMENTAIRES_DEMANDEUR COMMENTAIRES_DEMANDEUR,-- mis en commentaire pour ne pas avoir de doublon , POTC le 06/02/25
            FLAG_HORS_BACKLOG FLAG_HORS_BACKLOG
         FROM commande_hors_backlog_bz
      )
      SELECT
         OU_NAME,
         CDE,
         LGN_CDE,
         line_number,
         shipment_number,
         -- DEMANDE_PAR,              -- mis en commentaire pour ne pas avoir de doublon , POTC le 06/02/25
         -- DATE_DEMANDE,             -- mis en commentaire pour ne pas avoir de doublon , POTC le 06/02/25
         -- NO_INCIDENT,              -- mis en commentaire pour ne pas avoir de doublon , POTC le 06/02/25
         -- CDE_REMPLACEMENT,         -- mis en commentaire pour ne pas avoir de doublon , POTC le 06/02/25
         -- COMMENTAIRES_DEMANDEUR,   -- mis en commentaire pour ne pas avoir de doublon , POTC le 06/02/25
         FLAG_HORS_BACKLOG,
         ROWNUM ROW_NUMBER_ID,
         SYSDATE ROW_CREATION_DATE,
         SYSDATE ROW_LAST_UPDATE_DATE
      FROM vw
   ;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'COMMANDE_HORS_BACKLOG';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('COMMANDE_HORS_BACKLOG');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
   Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END COMMANDE_HORS_BACKLOG_PROC;

/****************************************************************************************
* PROCEDURE   :  ORDER_CHARGES_PROC
* DESCRIPTION :  Create table ORDER_CHARGES
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE ORDER_CHARGES_PROC
IS
   v_procedure varchar2(100) := 'ORDER_CHARGES_PROC';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'ORDER_CHARGES';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
      EXECUTE IMMEDIATE 'TRUNCATE TABLE ORDER_CHARGES';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'ORDER_CHARGES';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO ORDER_CHARGES
   SELECT
      line_id,
      ARITHMETIC_OPERATOR,
      OPERAND,
      charge_type_code,
      ADJUSTED_AMOUNT
   FROM STEPH_APPS_OE_PRICE_ADJUSTMENTS_bz
   WHERE
      (ARITHMETIC_OPERATOR = 'LUMPSUM' OR ARITHMETIC_OPERATOR = '%')
      AND list_line_type_code = 'FREIGHT_CHARGE'
   ;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'ORDER_CHARGES';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('ORDER_CHARGES');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
   Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END ORDER_CHARGES_PROC;

/****************************************************************************************
* PROCEDURE   :  UOM_CONVERSION_RATE_PROC
* DESCRIPTION :  Create table UOM_CONVERSION_RATE
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE UOM_CONVERSION_RATE_PROC
IS
   v_procedure varchar2(100) := 'UOM_CONVERSION_RATE_PROC';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'UOM_CONVERSION_RATE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE UOM_CONVERSION_RATE';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'UOM_CONVERSION_RATE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
   INSERT INTO UOM_CONVERSION_RATE
   SELECT
      samuc.UNIT_OF_MEASURE,
      samuc.UOM_CODE,
      samuc.UOM_CLASS,
      samuc.INVENTORY_ITEM_ID,
      samuc.CONVERSION_RATE,
      ROWNUM ROW_NUMBER_ID,
      SYSDATE ROW_CREATION_DATE,
      SYSDATE ROW_LAST_UPDATE_DATE
   FROM STEPH_APPS_MTL_UOM_CONVERSIONS_BZ samuc
   ;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'UOM_CONVERSION_RATE';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('UOM_CONVERSION_RATE');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
   Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END  UOM_CONVERSION_RATE_PROC;

/****************************************************************************************
   * PROCEDURE   :  DOCUMENT_EIA_LINE_TEXT_TEMP_PROC
   * DESCRIPTION :  Create table DOCUMENT_EIA_LINE_TEXT_TEMP
   * PARAMETRES  :
   * NOM               TYPE        DESCRIPTION
   * -------------------------------------------------------------------------------------
   * <parameter>      <TYPE>      <Desc>
   ****************************************************************************************/

PROCEDURE DOCUMENT_EIA_LINE_TEXT_TEMP_PROC
IS
   v_procedure varchar2(100) := 'DOCUMENT_EIA_LINE_TEXT_TEMP_PROC';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'DOCUMENT_EIA_LINE_TEXT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE DOCUMENT_EIA_LINE_TEXT_TEMP';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'DOCUMENT_EIA_LINE_TEXT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';
   INSERT INTO DOCUMENT_EIA_LINE_TEXT_TEMP
   SELECT
      fndd2.pk1_value AS LINE_IDA,
      LISTAGG('Date ' || TO_CHAR(fnd2.last_update_date, 'DD/MM/YY HH24:MI:SS') || ' -> ' || fns2.short_text, ' # ') WITHIN GROUP (ORDER BY fnd2.last_update_date) AS EIA_LINE_TEXT
   FROM steph_apps_fnd_attached_documents_bz fndd2
   INNER JOIN steph_apps_fnd_documents_bz fnd2
      ON fnd2.document_id = fndd2.document_id
   INNER JOIN steph_apps_fnd_documents_short_text_bz fns2
      ON fnd2.media_id = fns2.media_id
   INNER JOIN (SELECT category_id FROM steph_apps_fnd_document_categories_tl_bz WHERE user_name = 'EIA Line Text' AND language = 'US') fdct
      ON fndd2.category_id = fdct.category_id
   GROUP BY fndd2.pk1_value
   ORDER BY fndd2.pk1_value;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'DOCUMENT_EIA_LINE_TEXT_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('DOCUMENT_EIA_LINE_TEXT_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
   Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END  DOCUMENT_EIA_LINE_TEXT_TEMP_PROC;

/****************************************************************************************
* PROCEDURE   :  MANUAL_ADJUSTMENTS_TEMP_PROC
* DESCRIPTION :  Create table MANUAL_ADJUSTMENTS_TEMP
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE MANUAL_ADJUSTMENTS_TEMP_PROC
IS
   v_procedure varchar2(100) := 'MANUAL_ADJUSTMENTS_TEMP_PROC';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'MANUAL_ADJUSTMENTS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE MANUAL_ADJUSTMENTS_TEMP';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'MANUAL_ADJUSTMENTS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;

   INSERT INTO MANUAL_ADJUSTMENTS_TEMP (
      BU_INVOICE,
      BUSINESS_SEGMENT_INVOICE_NEW,
      PERIMETRE_LS_CONSO,
      FLAG_ITEM_LS_SALES,
      TRX_LOCATION_GL_REGROUPEMENT_VENTE,
      OU_NAME,
      TRX_ACCOUNT_LOCATION_GL_SGT2,
      TRX_INTERCOMPANY_TYPE,
      KEY_INVOICE_BILL_TO,
      ITEM_CODE,
      GL_DATE,
      TRX_DATE,
      INVOICE_CURRENCY_CODE,
      --INVOICED_AMOUNT_IN_TRANSACTIONAL_CURRENCY,
      INVOICED_AMOUNT_IN_USD_FIXED,
      DATE_MAJ,
      COMMENTAIRES,
      FETCH_DATE,
      FETCH_YEAR,
      FETCH_MONTH,
      FETCH_DAY,
      ROW_NUMBER_ID,
      ROW_CREATION_DATE,
      ROW_LAST_UPDATE_DATE
   )
   SELECT
      BU_INVOICE,
      BUSINESS_SEGMENT_INVOICE_NEW,
      PERIMETRE_LS_CONSO,
      FLAG_ITEM_LS_SALES,
      TRX_LOCATION_GL_REGROUPEMENT_VENTE,
      OU_NAME,
      TRX_ACCOUNT_LOCATION_GL_SGT2,
      TRX_INTERCOMPANY_TYPE,
      KEY_INVOICE_BILL_TO,
      ITEM_CODE,
      GL_DATE,
      TRX_DATE,
      INVOICE_CURRENCY_CODE,
      --INVOICED_AMOUNT_IN_TRANSACTIONAL_CURRENCY,
      INVOICED_AMOUNT_IN_USD_FIXED,
      DATE_MAJ,
      COMMENTAIRES,
      FETCH_DATE,
      FETCH_YEAR,
      FETCH_MONTH,
      FETCH_DAY,
      ROWNUM AS ROW_NUMBER_ID,
      SYSDATE AS ROW_CREATION_DATE,
      SYSDATE AS ROW_LAST_UPDATE_DATE
   FROM FILE_CIMD_MANUAL_ADJUSTMENTS_BZ f;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'MANUAL_ADJUSTMENTS_TEMP';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('MANUAL_ADJUSTMENTS_TEMP');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
   Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END  MANUAL_ADJUSTMENTS_TEMP_PROC;

/****************************************************************************************
* PROCEDURE   :  FILE_VAR_BUSINESS_SEGMENT_NEW_PROC
* DESCRIPTION :  Create table FILE_VAR_BUSINESS_SEGMENT_NEW
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE FILE_VAR_BUSINESS_SEGMENT_NEW_PROC
IS
   v_procedure varchar2(100) := 'FILE_VAR_BUSINESS_SEGMENT_NEW';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'FILE_VAR_BUSINESS_SEGMENT_NEW';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE FILE_VAR_BUSINESS_SEGMENT_NEW';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'FILE_VAR_BUSINESS_SEGMENT_NEW';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO FILE_VAR_BUSINESS_SEGMENT_NEW
   SELECT
      BUSINESS_SEGMENT_INVOICE_NEW,
      PRIORITY,
      BU_INVOICE,
      SUBBU_ORDER,
      TRX_ACCOUNT_LOCATION_GL_SGT2,
      ACCOUNT_NUMBER_BILL_TO,
      PARENT_COMPANY_BILL_TO,
      COMMENTS,
      DATE_MAJ,
      FETCH_DATE,
      FETCH_YEAR,
      FETCH_MONTH,
      FETCH_DAY,
      rownum  ROW_NUMBER_ID,
      sysdate ROW_CREATION_DATE,
      sysdate ROW_LAST_UPDATE_DATE,
      SITE_FANO
   FROM FILE_VAR_BUSINESS_SEGMENT_NEW_BZ;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'FILE_VAR_BUSINESS_SEGMENT_NEW';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('FILE_VAR_BUSINESS_SEGMENT_NEW');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END  FILE_VAR_BUSINESS_SEGMENT_NEW_PROC;

/****************************************************************************************
* PROCEDURE   :  FILE_VAR_BUSINESS_SEGMENT_OLD_PROC
* DESCRIPTION :  Create table  FILE_VAR_BUSINESS_SEGMENT_OLD
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE FILE_VAR_BUSINESS_SEGMENT_OLD_PROC
IS
   v_procedure varchar2(100) := 'FILE_VAR_BUSINESS_SEGMENT_OLD';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'FILE_VAR_BUSINESS_SEGMENT_OLD';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE FILE_VAR_BUSINESS_SEGMENT_OLD';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'FILE_VAR_BUSINESS_SEGMENT_OLD';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO FILE_VAR_BUSINESS_SEGMENT_OLD
   SELECT
      fso.*,
      rownum  ROW_NUMBER_ID,
      sysdate ROW_CREATION_DATE,
      sysdate ROW_LAST_UPDATE_DATE
   FROM FILE_VAR_BUSINESS_SEGMENT_OLD_BZ fso;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'FILE_VAR_BUSINESS_SEGMENT_OLD';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('FILE_VAR_BUSINESS_SEGMENT_OLD');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
   Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END  FILE_VAR_BUSINESS_SEGMENT_OLD_PROC;

/****************************************************************************************
* PROCEDURE   :  steph_apps_fnd_flex_value_tl_PROC
* DESCRIPTION :  Create table  steph_apps_fnd_flex_values_tl
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE steph_apps_fnd_flex_value_tl_PROC
IS
   v_procedure varchar2(100) := 'steph_apps_fnd_flex_value_tl_PROC';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'steph_apps_fnd_flex_values_tl';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE steph_apps_fnd_flex_values_tl';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'steph_apps_fnd_flex_values_tl';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO steph_apps_fnd_flex_values_tl
   SELECT *
   FROM steph_apps_fnd_flex_values_tl_bz
   WHERE language = 'US';

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'steph_apps_fnd_flex_values_tl';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('steph_apps_fnd_flex_values_tl');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END  steph_apps_fnd_flex_value_tl_PROC;

/****************************************************************************************
* PROCEDURE   :  Fnd_flex_values_us_proc
* DESCRIPTION :  Create table  FND_FLEX_VALUES_US
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE Fnd_flex_values_us_proc
IS
   v_procedure varchar2(100) := 'Fnd_flex_values_us_proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'FND_FLEX_VALUES_US';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE FND_FLEX_VALUES_US';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'FND_FLEX_VALUES_US';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line';

   INSERT INTO FND_FLEX_VALUES_US
   SELECT
      ffs.BZ_BIS_CREATED_BY               as BZ_BIS_CREATED_BY_ffv_set,
      ffs.BZ_BIS_CREATION_DATE            as BZ_BIS_CREATION_DATE_ffv_set,
      ffs.BZ_BIS_UPTATED_BY               as BZ_BIS_UPTATED_BY_ffv_set,
      ffs.BZ_BIS_UPDATED_DATE             as BZ_BIS_UPDATED_DATE_ffv_set,
      ffs.FETCH_YEAR                      as FETCH_YEAR_ffv_set,
      ffs.FETCH_MONTH                     as FETCH_MONTH_ffv_set,
      ffs.FETCH_DAY                       as FETCH_DAY_ffv_set,
      ffs.FLEX_VALUE_SET_ID               as FLEX_VALUE_SET_ID_ffv_set,
      ffs.FLEX_VALUE_SET_NAME             as FLEX_VALUE_SET_NAME_ffv_set,
      ffs.LAST_UPDATE_DATE                as LAST_UPDATE_DATE_ffv_set,
      ffs.LAST_UPDATED_BY                 as LAST_UPDATED_BY_ffv_set,
      ffs.CREATION_DATE                   as CREATION_DATE_ffv_set,
      ffs.CREATED_BY                      as CREATED_BY_ffv_set,
      ffs.LAST_UPDATE_LOGIN               as LAST_UPDATE_LOGIN_ffv_set,
      ffs.VALIDATION_TYPE                 as VALIDATION_TYPE_ffv_set,
      ffs.PROTECTED_FLAG                  as PROTECTED_FLAG_ffv_set,
      ffs.SECURITY_ENABLED_FLAG           as SECURITY_ENABLED_FLAG_ffv_set,
      ffs.LONGLIST_FLAG                   as LONGLIST_FLAG_ffv_set,
      ffs.FORMAT_TYPE                     as FORMAT_TYPE_ffv_set,
      ffs.MAXIMUM_SIZE                    as MAXIMUM_SIZE_ffv_set,
      ffs.ALPHANUMERIC_ALLOWED_FLAG       as ALPHANUMERIC_ALLOWED_FLAG_ffv_set,
      ffs.UPPERCASE_ONLY_FLAG             as UPPERCASE_ONLY_FLAG_ffv_set,
      ffs.NUMERIC_MODE_ENABLED_FLAG       as NUMERIC_MODE_ENABLED_FLAG_ffv_set,
      ffs.DESCRIPTION                     as DESCRIPTION_ffv_set,
      ffs.DEPENDANT_DEFAULT_VALUE         as DEPENDANT_DEFAULT_VALUE_ffv_set,
      ffs.DEPENDANT_DEFAULT_MEANING       as DEPENDANT_DEFAULT_MEANING_ffv_set,
      ffs.PARENT_FLEX_VALUE_SET_ID        as PARENT_FLEX_VALUE_SET_ID_ffv_set,
      ffs.MINIMUM_VALUE                   as MINIMUM_VALUE_ffv_set,
      ffs.MAXIMUM_VALUE                   as MAXIMUM_VALUE_ffv_set,
      ffs.NUMBER_PRECISION                as NUMBER_PRECISION_ffv_set,
      ffs.ZD_EDITION_NAME                 as ZD_EDITION_NAME_ffv_set,
      ffs.ZD_SYNC                         as ZD_SYNC_ffv_set,
      ffs.FETCH_DATE                      as FETCH_DATE_ffv_set,

      ffv.BZ_BIS_CREATED_BY               as BZ_BIS_CREATED_BY_ffv_values,
      ffv.BZ_BIS_CREATION_DATE            as BZ_BIS_CREATION_DATE_ffv_values,
      ffv.BZ_BIS_UPTATED_BY               as BZ_BIS_UPTATED_BY_ffv_values,
      ffv.BZ_BIS_UPDATED_DATE             as BZ_BIS_UPDATED_DATE_ffv_values,
      ffv.FETCH_YEAR                      as FETCH_YEAR_ffv_values,
      ffv.FETCH_MONTH                     as FETCH_MONTH_ffv_values,
      ffv.FETCH_DAY                       as FETCH_DAY_ffv_values,
      ffv.FLEX_VALUE_SET_ID               as FLEX_VALUE_SET_ID_ffv_values,
      ffv.FLEX_VALUE_ID                   as FLEX_VALUE_ID_ffv_values,
      ffv.FLEX_VALUE                      as FLEX_VALUE_ffv_values,
      ffv.LAST_UPDATE_DATE                as LAST_UPDATE_DATE_ffv_values,
      ffv.LAST_UPDATED_BY                 as LAST_UPDATED_BY_ffv_values,
      ffv.CREATION_DATE                   as CREATION_DATE_ffv_values,
      ffv.CREATED_BY                      as CREATED_BY_ffv_values,
      ffv.LAST_UPDATE_LOGIN               as LAST_UPDATE_LOGIN_ffv_values,
      ffv.ENABLED_FLAG                    as ENABLED_FLAG_ffv_values,
      ffv.SUMMARY_FLAG                    as SUMMARY_FLAG_ffv_values,
      ffv.START_DATE_ACTIVE               as START_DATE_ACTIVE_ffv_values,
      ffv.END_DATE_ACTIVE                 as END_DATE_ACTIVE_ffv_values,
      PARENT_FLEX_VALUE_LOW               as PARENT_FLEX_VALUE_LOW_ffv_values,
      ffv.PARENT_FLEX_VALUE_HIGH          as PARENT_FLEX_VALUE_HIGH_ffv_values,
      ffv.STRUCTURED_HIERARCHY_LEVEL      as STRUCTURED_HIERARCHY_LEVEL_ffv_values,
      ffv.HIERARCHY_LEVEL                 as HIERARCHY_LEVEL_ffv_values,
      ffv.COMPILED_VALUE_ATTRIBUTES       as COMPILED_VALUE_ATTRIBUTES_ffv_values,
      ffv.VALUE_CATEGORY                  as VALUE_CATEGORY_ffv_values,
      ffv.ATTRIBUTE1                      as ATTRIBUTE1_ffv_values,
      ffv.ATTRIBUTE2                      as ATTRIBUTE2_ffv_values,
      ffv.ATTRIBUTE3                      as ATTRIBUTE3_ffv_values,
      ffv.ATTRIBUTE4                      as ATTRIBUTE4_ffv_values,
      ffv.ATTRIBUTE5                      as ATTRIBUTE5_ffv_values,
      ffv.ATTRIBUTE6                      as ATTRIBUTE6_ffv_values,
      ffv.ATTRIBUTE7                      as ATTRIBUTE7_ffv_values,
      ffv.ATTRIBUTE8                      as ATTRIBUTE8_ffv_values,
      ffv.ATTRIBUTE9                      as ATTRIBUTE9_ffv_values,
      ffv.ATTRIBUTE10                     as ATTRIBUTE10_ffv_values,
      ffv.ATTRIBUTE11                     as ATTRIBUTE11_ffv_values,
      ffv.ATTRIBUTE12                     as ATTRIBUTE12_ffv_values,
      ffv.ATTRIBUTE13                     as ATTRIBUTE13_ffv_values,
      ffv.ATTRIBUTE14                     as ATTRIBUTE14_ffv_values,
      ffv.ATTRIBUTE15                     as ATTRIBUTE15_ffv_values,
      ffv.ATTRIBUTE16                     as ATTRIBUTE16_ffv_values,
      ffv.ATTRIBUTE17                     as ATTRIBUTE17_ffv_values,
      ffv.ATTRIBUTE18                     as ATTRIBUTE18_ffv_values,
      ffv.ATTRIBUTE19                     as ATTRIBUTE19_ffv_values,
      ffv.ATTRIBUTE20                     as ATTRIBUTE20_ffv_values,
      ffv.ATTRIBUTE21                     as ATTRIBUTE21_ffv_values,
      ffv.ATTRIBUTE22                     as ATTRIBUTE22_ffv_values,
      ffv.ATTRIBUTE23                     as ATTRIBUTE23_ffv_values,
      ffv.ATTRIBUTE24                     as ATTRIBUTE24_ffv_values,
      ffv.ATTRIBUTE25                     as ATTRIBUTE25_ffv_values,
      ffv.ATTRIBUTE26                     as ATTRIBUTE26_ffv_values,
      ffv.ATTRIBUTE27                     as ATTRIBUTE27_ffv_values,
      ffv.ATTRIBUTE28                     as ATTRIBUTE28_ffv_values,
      ffv.ATTRIBUTE29                     as ATTRIBUTE29_ffv_values,
      ffv.ATTRIBUTE30                     as ATTRIBUTE30_ffv_values,
      ffv.ATTRIBUTE31                     as ATTRIBUTE31_ffv_values,
      ffv.ATTRIBUTE32                     as ATTRIBUTE32_ffv_values,
      ffv.ATTRIBUTE33                     as ATTRIBUTE33_ffv_values,
      ffv.ATTRIBUTE34                     as ATTRIBUTE34_ffv_values,
      ffv.ATTRIBUTE35                     as ATTRIBUTE35_ffv_values,
      ffv.ATTRIBUTE36                     as ATTRIBUTE36_ffv_values,
      ffv.ATTRIBUTE37                     as ATTRIBUTE37_ffv_values,
      ffv.ATTRIBUTE38                     as ATTRIBUTE38_ffv_values,
      ffv.ATTRIBUTE39                     as ATTRIBUTE39_ffv_values,
      ffv.ATTRIBUTE40                     as ATTRIBUTE40_ffv_values,
      ffv.ATTRIBUTE41                     as ATTRIBUTE41_ffv_values,
      ffv.ATTRIBUTE42                     as ATTRIBUTE42_ffv_values,
      ffv.ATTRIBUTE43                     as ATTRIBUTE43_ffv_values,
      ffv.ATTRIBUTE44                     as ATTRIBUTE44_ffv_values,
      ffv.ATTRIBUTE45                     as ATTRIBUTE45_ffv_values,
      ffv.ATTRIBUTE46                     as ATTRIBUTE46_ffv_values,
      ffv.ATTRIBUTE47                     as ATTRIBUTE47_ffv_values,
      ffv.ATTRIBUTE48                     as ATTRIBUTE48_ffv_values,
      ffv.ATTRIBUTE49                     as ATTRIBUTE49_ffv_values,
      ffv.ATTRIBUTE50                     as ATTRIBUTE50_ffv_values,
      ffv.ATTRIBUTE_SORT_ORDER            as ATTRIBUTE_SORT_ORDER_ffv_values,
      ffv.ZD_EDITION_NAME                 as ZD_EDITION_NAME_ffv_values,
      ffv.ZD_SYNC                         as ZD_SYNC_ffv_values,
      ffv.FETCH_DATE                      as FETCH_DATE_ffv_values,

      ffvt.BZ_BIS_CREATED_BY              as BZ_BIS_CREATED_BY_ffv_tl,
      ffvt.BZ_BIS_CREATION_DATE           as BZ_BIS_CREATION_DATE_ffv_tl,
      ffvt.BZ_BIS_UPTATED_BY              as BZ_BIS_UPTATED_BY_ffv_tl,
      ffvt.BZ_BIS_UPDATED_DATE            as BZ_BIS_UPDATED_DATE_ffv_tl,
      ffvt.FLEX_VALUE_ID                  as FLEX_VALUE_ID_ffv_tl,
      ffvt.LANGUAGE                       as LANGUAGE_ffv_tl,
      ffvt.LAST_UPDATE_DATE               as LAST_UPDATE_DATE_ffv_tl,
      ffvt.LAST_UPDATED_BY                as LAST_UPDATED_BY_ffv_tl,
      ffvt.CREATION_DATE                  as CREATION_DATE_ffv_tl,
      ffvt.CREATED_BY                     as CREATED_BY_ffv_tl,
      ffvt.LAST_UPDATE_LOGIN              as LAST_UPDATE_LOGIN_ffv_tl,
      ffvt.DESCRIPTION                    as DESCRIPTION_ffv_tl,
      ffvt.SOURCE_LANG                    as SOURCE_LANG_ffv_tl,
      ffvt.FLEX_VALUE_MEANING             as FLEX_VALUE_MEANING_ffv_tl,
      ffvt.ZD_EDITION_NAME                as ZD_EDITION_NAME_ffv_tl,
      ffvt.ZD_SYNC                        as ZD_SYNC_ffv_tl,
      ffvt.FETCH_DATE                     as FETCH_DATE_ffv_tl,
      ffvt.FETCH_YEAR                     as FETCH_YEAR_ffv_tl,
      ffvt.FETCH_MONTH                    as FETCH_MONTH_ffv_tl,
      ffvt.FETCH_DAY                      as FETCH_DAY_ffv_tl,

      ROWNUM                              as ROW_NUMBER_ID,
      SYSDATE                             as ROW_CREATION_DATE,
      SYSDATE                             as ROW_LAST_UPDATE_DATE

   FROM steph_apps_fnd_flex_value_sets_bz ffs
   LEFT JOIN steph_apps_fnd_flex_values#_bz ffv
      ON ffs.flex_value_set_id = ffv.flex_value_set_id
   LEFT JOIN STEPH_APPS_FND_FLEX_VALUES_TL_bz ffvt
      ON ffv.flex_value_id = ffvt.flex_value_id
      AND ffvt.language = 'US';

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'FND_FLEX_VALUES_US';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('FND_FLEX_VALUES_US');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
      Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END  Fnd_flex_values_us_proc;

/****************************************************************************************
* PROCEDURE   :  FILE_VAR_BU_proc
* DESCRIPTION :  Create table  FILE_VAR_SUBBU
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE FILE_VAR_BU_proc
IS
   v_procedure varchar2(100) := 'FILE_VAR_BU_proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'FILE_VAR_BU';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE FILE_VAR_BU';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'FILE_VAR_BU';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;

   INSERT INTO FILE_VAR_BU
   SELECT
      fvb.*,
      ROWNUM  as ROW_NUMBER_ID,
      SYSDATE as ROW_CREATION_DATE,
      SYSDATE as ROW_LAST_UPDATE_DATE
   FROM FILE_VAR_BU_bz fvb;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'FILE_VAR_BU';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('FILE_VAR_BU');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
   Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END  FILE_VAR_BU_proc;

/****************************************************************************************
* PROCEDURE   :  FILE_VAR_SUBBU_proc
* DESCRIPTION :  Create table  FILE_VAR_SUBBU
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE FILE_VAR_SUBBU_proc
IS
   v_procedure varchar2(100) := 'FILE_VAR_SUBBU_proc';
   v_date_deb_proc  TIMESTAMP := sysdate;
BEGIN
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_error_code:=NULL;
   g_error_msg :=NULL;
   g_date_fin  :=NULL;
   g_rowcount  :=0;

   g_table     := v_procedure;
   g_date_deb  := v_date_deb_proc;
   g_status    := 'BEGIN';
   g_etape     := '0002 - Begin PROC';
   Write_Log_PROC;

   g_table     := 'FILE_VAR_SUBBU';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line' ;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE FILE_VAR_SUBBU';
   g_status   := 'COMPLETED';
   g_etape    := '011 - TRUNCATE TABLE' ;
   Write_Log_PROC;

   g_table     := 'FILE_VAR_SUBBU';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;

   INSERT INTO FILE_VAR_SUBBU
   SELECT
      fvs.*,
      ROWNUM  as ROW_NUMBER_ID,
      SYSDATE as ROW_CREATION_DATE,
      SYSDATE as ROW_LAST_UPDATE_DATE
   FROM FILE_VAR_SUBBU_bz fvs;

   g_status   := 'COMPLETED';
   g_etape    := '010 - INSERT INTO' ;
   Write_Log_PROC;

   g_table     := v_procedure;
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      COMMIT;
   g_status   := 'COMPLETED';
   g_etape    := '100 - COMMIT';
   Write_Log_PROC;

   g_table     := 'FILE_VAR_SUBBU';
   g_date_deb  := sysdate;
   g_status    := 'WIP';
   g_etape     := $$plsql_line + 1  || ' - num error line'  ;
      LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('FILE_VAR_SUBBU');
   g_status   := 'COMPLETED';
   g_etape    := '099 - STATS' ;
   Write_Log_PROC;

   g_table    := v_procedure;
   g_date_deb := v_date_deb_proc;
   g_status   := 'END SUCCESS';
   g_etape    := '9992 - End PROC';
   Write_Log_PROC;

EXCEPTION
   WHEN OTHERS THEN
   Exceptions_PROC;

      g_table     := v_procedure;
      g_date_deb  := sysdate;
      g_status    := 'WIP';
      g_etape     := $$plsql_line + 1  || ' - num error line'  ;
         ROLLBACK;
      g_status   :='COMPLETED';
      g_etape    := '111 - ROLLBACK';
      Write_Log_PROC;

      g_table    := v_procedure;
      g_date_deb := v_date_deb_proc;
      g_status   := 'END FAILED';
      g_etape    := '9992 - End PROC';
      Write_Log_PROC;

END  FILE_VAR_SUBBU_proc;

/****************************************************************************************
* PROCEDURE   :  MAIN
* DESCRIPTION :  Procedure principale
*
* PARAMETRES  :
* NOM               TYPE        DESCRIPTION
* -------------------------------------------------------------------------------------
* <parameter>      <TYPE>      <Desc>
****************************************************************************************/

PROCEDURE MAIN (
   pv_errbuf   OUT VARCHAR2,
   pn_retcode  OUT NUMBER
)
IS
   --variables
   v_procedure            varchar2(100)   := 'MAIN';
   v_status               varchar2(1)     := 'A';  --statut Accepté (sinon R pour Rejeté)
   v_message              varchar2(1000)  := NULL;
   v_date_deb_pkg         TIMESTAMP := sysdate;

BEGIN  --Début traitement
   DBMS_OUTPUT.ENABLE (1000000);

      -- g_programme := 'LH2_DTH_SILVER_CORE_PKG.MAIN';
   g_level := 'S';
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_table     := $$plsql_unit;
   g_date_deb  := v_date_deb_pkg;
   g_status   := 'BEGIN';
   g_etape    := '0001 - Begin PKG';
   Write_Log_PROC;

   DBMS_OUTPUT.PUT_LINE (g_programme);
   DBMS_OUTPUT.PUT_LINE ('-----------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE ('--------------------------START----------------------------------');

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Gl_Account_Details_Proc');
   Recreate_Gl_Account_Details_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_IO_Details_Proc');
   Recreate_IO_Details_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_OU_Details_Proc');
   Recreate_OU_Details_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Structure_Reseau_Proc');
   Recreate_Structure_Reseau_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Structure_Reseau_Proc');
   Recreate_Intercompany_Parameters_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Gl_Segment1_Proc');
   Recreate_Gl_Segment1_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Gl_Segment2_Proc');
   Recreate_Gl_Segment2_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Gl_Segment3_Proc');
   Recreate_Gl_Segment3_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Gl_Segment4_Proc');
   Recreate_Gl_Segment4_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Gl_Segment5_Proc');
   Recreate_Gl_Segment5_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Gl_Segment6_Proc');
   Recreate_Gl_Segment6_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Daily_conversion_rates_proc');
   Daily_conversion_rates_proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Fixed_rate_Proc');
   Fixed_rate_Proc;

      v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Country_Zone_Proc');
   Recreate_Country_Zone_Proc;

      v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Recreate_Customer_Segement_Proc');
   Recreate_Customer_Segement_Proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de COMMANDE_HORS_BACKLOG_PROC');
   COMMANDE_HORS_BACKLOG_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de ORDER_CHARGES_PROC');
   ORDER_CHARGES_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de UOM_CONVERSION_RATE_PROC');
   UOM_CONVERSION_RATE_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de DOCUMENT_EIA_LINE_TEXT_TEMP_PROC');
   DOCUMENT_EIA_LINE_TEXT_TEMP_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de MANUAL_ADJUSTMENTS_TEMP_PROC');
   MANUAL_ADJUSTMENTS_TEMP_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de FILE_VAR_BUSINESS_SEGMENT_NEW_PROC;');
   FILE_VAR_BUSINESS_SEGMENT_NEW_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de FILE_VAR_BUSINESS_SEGMENT_OLD_PROC;');
   FILE_VAR_BUSINESS_SEGMENT_OLD_PROC;


   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de steph_apps_fnd_flex_value_tl_PROC');
   steph_apps_fnd_flex_value_tl_PROC;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de Fnd_flex_values_us_proc');
   Fnd_flex_values_us_proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de FILE_VAR_BU_proc');
   FILE_VAR_BU_proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('Lancement de FILE_VAR_SUBBU_proc');
   FILE_VAR_SUBBU_proc;

   v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
   DBMS_OUTPUT.PUT_LINE (v_message);
   DBMS_OUTPUT.PUT_LINE ('-----------------------------END--------------------------------------');--fin compte-rendu

   if g_erreur_pkg = 1
   then
   pn_retcode  := 1;
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_table     := $$plsql_unit;
   g_date_deb  := v_date_deb_pkg;
   g_status   := 'END FAILED';
   g_etape    := '9991 - End PKG';
   Write_Log_PROC;
   else
   g_programme := $$plsql_unit || '.' || v_procedure ;
   g_table     := $$plsql_unit;
   g_date_deb  := v_date_deb_pkg;
   g_status   := 'END SUCCESS';
   g_etape    := '9991 - End PKG';
   Write_Log_PROC;
   end if ;

EXCEPTION
   WHEN OTHERS THEN
      pn_retcode := 1;
      pv_errbuf := SQLCODE || '-' || SQLERRM;
      Exceptions_PROC;

      g_programme := $$plsql_unit || '.' || v_procedure ;
      g_table     := $$plsql_unit;
      g_date_deb  := v_date_deb_pkg;
      g_status   := 'END FAILED';
      g_etape    := '9991 - End PKG';
      Write_Log_PROC;

END MAIN;

END LH2_DTH_SILVER_CORE_PKG;