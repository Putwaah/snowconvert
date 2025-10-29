create or replace PACKAGE BODY LH2_DTH_SILVER_PURCHASE_ORDER_PKG IS

/*     $Header: LH2_DTH_SILVER_PURCHASE_ORDER_PKG.sql 1.0.0 2025/01/21 09:00:00 vsre noship $ */
-- ***************************************************************************
-- @(#) ----------------------------------------------------------------------
-- @(#) Specifique: DTH005 IMPORT des donn�es dans le Silver - DataHub Customer
-- @(#) Fichier   : .LH2_DTH_PURCHASE_ORDER_PKG.sql
-- @(#) Version   : 1.0.0 du 11/06/2024
-- @(#) ---------------------------------------------e-------------------------
-- Objet          : Package Body du DTH005 IMPORT des donn�es Silver
-- Commentaires   :
-- Exemple        :
-- ***************************************************************************
--                            HISTORIQUE DES VERSIONS
-- ---------------------------------------------------------------------------
-- Date     Version  Nom            Description de l'intervention
-- -------- -------- -------------- ------------------------------------------
-- 21/01/25  1.0.0   GOICHON  Version initiale
-- 03/02/25  1.0.1   GOICHON  CHANGEMENT DE NOM DE CERTAINE VARIABLE
-- 27/02/25  2.0.0   GOICHON  Modification de BU_NEW et filtre sur la date
-- 17/03/25  2.0.   POTC     D�placement de la cr�ation du calcul des stats dans un package s�par�
-- 07/07/25  2.0.1  GOICHON  Ajout de LEAD_TIME_TO_NAMED_PLACE_LS � po_standard , po_blanket , requisition_internal  --> chargement enlev� car doublon � �tudier
-- 27/08/25  2.0.2   OJABIT         Ajout des hint
-- 29/09/25  2.0.3  GOICHON   remplacement des jointures avec FILE_VAR_BU_BZ par la jointure avec FILE_TRANSFORMATION_DATA
-- 13/10/25  2.0.4  POTC        Modif jointure FND_USER pour la table PO_CONTRACT
-- ***************************************************************************

/****************************************************************************************
    * PROCEDURE   :  Exceptions_PROC
    * DESCRIPTION :  Procedure g�n�rique pour les exceptions
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
    * DESCRIPTION :  Procedure g�n�rique pour la g�n�ration des Logs
    *                
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------*/

   PROCEDURE Write_Log_PROC 
   IS
   BEGIN  --D�but traitement

   -- g_status    :='COMPLETED';
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

	--	g_date_deb  :=sysdate;

   END Write_Log_PROC;
   
   
   
 /****************************************************************************************
    * PROCEDURE   :  HINT test
    * DESCRIPTION :  Create PURCHASE_INVOICE TABLE
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/  
PROCEDURE HINT_ON_PROC
IS
    v_procedure      VARCHAR2(100) := 'HINT_ON_PROC';
    v_date_deb_proc  TIMESTAMP := SYSDATE;
    v_sql            VARCHAR2(1000);
BEGIN
    g_programme := $$plsql_unit || '.' || v_procedure;
    g_error_code := NULL;
    g_error_msg  := NULL;
    g_date_fin   := NULL;
    g_rowcount   := 0;

    g_date_deb := v_date_deb_proc;
    g_status   := 'BEGIN';
    g_etape    := '0002 - Begin PROC';
    Write_Log_PROC;

    -- �tape 1 : Activer les HINTs
    BEGIN
        v_sql := 'ALTER SESSION SET OPTIMIZER_IGNORE_HINTS = FALSE';
        g_etape := 'Activer OPTIMIZER_IGNORE_HINTS';
        DBMS_OUTPUT.PUT_LINE('Execution : ' || v_sql);
        EXECUTE IMMEDIATE v_sql;
        g_status := 'SUCCESS';
        g_error_msg := 'OPTIMIZER_IGNORE_HINTS activ� avec succ�s';
        Write_Log_PROC;
    EXCEPTION
        WHEN OTHERS THEN
            g_status := 'ERROR';
            g_error_code := SQLCODE;
            g_error_msg := 'Erreur sur OPTIMIZER_IGNORE_HINTS : ' || SQLERRM;
            Write_Log_PROC;
            RAISE;
    END;

-- �tape 2 : Activer les HINTs parall�les
BEGIN
    v_sql := 'ALTER SESSION SET OPTIMIZER_IGNORE_PARALLEL_HINTS = FALSE';
    g_etape := 'Activer OPTIMIZER_IGNORE_PARALLEL_HINTS';
    DBMS_OUTPUT.PUT_LINE('Execution : ' || v_sql);
    EXECUTE IMMEDIATE v_sql;
    g_status := 'SUCCESS';
    g_error_msg := 'OPTIMIZER_IGNORE_PARALLEL_HINTS activ� avec succ�s';
    Write_Log_PROC;

EXCEPTION
    WHEN OTHERS THEN
        g_status := 'ERROR';
        g_error_code := SQLCODE;
        g_error_msg := 'Erreur sur OPTIMIZER_IGNORE_PARALLEL_HINTS : ' || SQLERRM;
        Write_Log_PROC;
        RAISE;
END;

    -- �tape 3 : D�sactiver Auto DOP et limiter les serveurs parall�les
    BEGIN
        v_sql := 'ALTER SESSION FORCE PARALLEL QUERY';
        g_etape := 'D�sactiver Auto DOP';
        DBMS_OUTPUT.PUT_LINE('Execution : ' || v_sql);
        EXECUTE IMMEDIATE v_sql;

        g_status := 'SUCCESS';
        g_error_msg := 'Param�tres de parall�lisme configur�s avec succ�s';
        Write_Log_PROC;
    EXCEPTION
        WHEN OTHERS THEN
            g_status := 'ERROR';
            g_error_code := SQLCODE;
            g_error_msg := 'Erreur sur configuration parall�lisme : ' || SQLERRM;
            Write_Log_PROC;
            RAISE;
    END;

    -- �tape 4 : Activer le DML parall�le
    BEGIN
        v_sql := 'ALTER SESSION FORCE PARALLEL DML PARALLEL 6';
        g_etape := 'Activer PARALLEL DML';
        DBMS_OUTPUT.PUT_LINE('Execution : ' || v_sql);
        EXECUTE IMMEDIATE v_sql;
        g_status := 'SUCCESS';
        g_error_msg := 'PARALLEL DML activ� avec succ�s';
        Write_Log_PROC;
    EXCEPTION
        WHEN OTHERS THEN
            g_status := 'ERROR';
            g_error_code := SQLCODE;
            g_error_msg := 'Erreur sur PARALLEL DML : ' || SQLERRM;
            Write_Log_PROC;
            RAISE;
    END;

    -- �tape 5 : Activer la collecte compl�te des statistiques
    BEGIN
        v_sql := 'ALTER SESSION SET STATISTICS_LEVEL = ALL';
        g_etape := 'Activer STATISTICS_LEVEL = ALL';
        DBMS_OUTPUT.PUT_LINE('Execution : ' || v_sql);
        EXECUTE IMMEDIATE v_sql;
        g_status := 'SUCCESS';
        g_error_msg := 'STATISTICS_LEVEL = ALL activ� avec succ�s';
        Write_Log_PROC;
    EXCEPTION
        WHEN OTHERS THEN
            g_status := 'ERROR';
            g_error_code := SQLCODE;
            g_error_msg := 'Erreur sur STATISTICS_LEVEL : ' || SQLERRM;
            Write_Log_PROC;
            RAISE;
    END;


    -- �tape 6 :D�sactiver le plan baseline 
  /*  BEGIN
        v_sql := 'ALTER SESSION SET OPTIMIZER_USE_SQL_PLAN_BASELINES = FALSE';
        g_etape := 'D�sactiver OPTIMIZER_USE_SQL_PLAN_BASELINES = FALSE';
        DBMS_OUTPUT.PUT_LINE('Execution : ' || v_sql);
        EXECUTE IMMEDIATE v_sql;
        g_status := 'SUCCESS';
        g_error_msg := 'STATISTICS_LEVEL = ALL activ� avec succ�s';
        Write_Log_PROC;
    EXCEPTION
        WHEN OTHERS THEN
            g_status := 'ERROR';
            g_error_code := SQLCODE;
            g_error_msg := 'Erreur sur OPTIMIZER_USE_SQL_PLAN_BASELINES = FALSE ' || SQLERRM;
            Write_Log_PROC;
            RAISE;
    END;*/

    g_status := 'COMPLETED';
    g_etape  := 'HINT_ON_PROC termin�';
    Write_Log_PROC;

EXCEPTION
    WHEN OTHERS THEN
        g_status := 'FAILED';
        g_error_code := SQLCODE;
        g_error_msg := SQLERRM;
        Write_Log_PROC;
        RAISE;
END HINT_ON_PROC;

  /****************************************************************************************
    * PROCEDURE   :  PO_BLANKET_PROC
    * DESCRIPTION :  Create PO_BLANKET TABLE
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
   PROCEDURE PO_BLANKET_PROC
   IS   v_procedure varchar2(100) := 'PO_BLANKET_PROC';
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

     g_table     := 'PO_BLANKET'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE PO_BLANKET';
    HINT_ON_PROC;
     g_status   := 'COMPLETED';
     g_etape    := '011 - TRUNCATE TABLE' ;
     Write_Log_PROC;

     g_table     := 'PO_BLANKET'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'   ;
        INSERT /*+
         APPEND 
    PARALLEL(PO_BLANKET 6) 
    MONITOR 
        gather_plan_statistics
        */INTO  PO_BLANKET
                      select /*+
                      PARALLEL(pda 6)
                      PARALLEL(pll 6)
                      PARALLEL(pr 6)
                      PARALLEL(pl 6)
                      PARALLEL(ph 6)
                      PARALLEL(fupr 6)
                      PARALLEL(fuprb 6)
                      PARALLEL(fuprh 6)
                      PARALLEL(fuprc 6)
                      PARALLEL(fuph 6)
                      PARALLEL(fuphb 6)
                      PARALLEL(aat 6)
                      PARALLEL(odt 6)
                      PARALLEL(fupl 6)
                      PARALLEL(fuplb 6)
                      PARALLEL(fuplc 6)
                      PARALLEL(fuplcl 6)
                      PARALLEL(fuplcl 6)
                      PARALLEL(fupll 6)
                      PARALLEL(fupllb 6)
                      PARALLEL(fupllc 6)
                      PARALLEL(idt 6)
                      PARALLEL(fupda 6)
                      PARALLEL(fupdab 6)
                      PARALLEL(gcck 6)
                      PARALLEL(HLA 6)
                      PARALLEL(HLA2 6)
                      PARALLEL(sgt5 6)
                      PARALLEL(fvb 6)
                      PARALLEL(sgt2 6)
                      PARALLEL(qr 6)
        gather_plan_statistics
    */
                PH.PO_HEADER_ID       PH_PO_HEADER_ID,
                PH.AGENT_ID PH_AGENT_ID,
                LH2_DTH_SILVER_FUNCTIONS_PKG.GET_STEPH_APPS_PER_ALL_PEOPLE_F_NAME_FUNC(PH.AGENT_ID)       PO_BUYER,
                PH.TYPE_LOOKUP_CODE       PH_TYPE_LOOKUP_CODE,
                PH.LAST_UPDATE_DATE       PH_LAST_UPDATE_DATE,
                fuph.user_name      PH_LAST_UPDATED_BY,
                PH.SEGMENT1       PO_NUMBER,
                PH.CREATION_DATE       PH_CREATION_DATE,
                fuph.user_name       PH_CREATED_BY,
                PH.VENDOR_ID       PH_VENDOR_ID,
                PH.VENDOR_SITE_ID       PH_VENDOR_SITE_ID,
                PH.SHIP_TO_LOCATION_ID       PH_SHIP_TO_LOCATION_ID,
                PH.BILL_TO_LOCATION_ID       PH_BILL_TO_LOCATION_ID,
                PH.TERMS_ID PH_TERMS_ID,
                aat.name       PH_TERMS_NAME,
                PH.SHIP_VIA_LOOKUP_CODE       PH_SHIP_VIA_LOOKUP_CODE,
                PH.FOB_LOOKUP_CODE       PH_FOB_LOOKUP_CODE,
                PH.FREIGHT_TERMS_LOOKUP_CODE       PH_FREIGHT_TERMS_LOOKUP_CODE,
                PH.CURRENCY_CODE       PH_CURRENCY_CODE,
                PH.RATE_TYPE       PH_RATE_TYPE,
                PH.RATE_DATE       PH_RATE_DATE,
                PH.RATE       PH_RATE,
                PH.BLANKET_TOTAL_AMOUNT       PH_BLANKET_TOTAL_AMOUNT,
                PH.AUTHORIZATION_STATUS       PH_AUTHORIZATION_STATUS,
                PH.REVISION_NUM       PH_REVISION_NUM,
                PH.REVISED_DATE       PH_REVISED_DATE,
                PH.APPROVED_FLAG       PH_APPROVED_FLAG,
                PH.APPROVED_DATE       PH_APPROVED_DATE,
                PH.AMOUNT_LIMIT       PH_AMOUNT_LIMIT,
                PH.MIN_RELEASE_AMOUNT       PH_MIN_RELEASE_AMOUNT,
                PH.NOTE_TO_VENDOR       PH_NOTE_TO_VENDOR,
                PH.NOTE_TO_RECEIVER       PH_NOTE_TO_RECEIVER,
                PH.PRINT_COUNT       PH_PRINT_COUNT,
                PH.PRINTED_DATE       PH_PRINTED_DATE,
                PH.CONFIRMING_ORDER_FLAG       PH_CONFIRMING_ORDER_FLAG,
                PH.COMMENTS       PH_COMMENTS,
                PH.ACCEPTANCE_REQUIRED_FLAG       PH_ACCEPTANCE_REQUIRED_FLAG,
                PH.CLOSED_DATE       PH_CLOSED_DATE,
                PH.USER_HOLD_FLAG       PH_USER_HOLD_FLAG,
                PH.APPROVAL_REQUIRED_FLAG       PH_APPROVAL_REQUIRED_FLAG,
                PH.CANCEL_FLAG       PH_CANCEL_FLAG,
                PH.ATTRIBUTE_CATEGORY       PH_ATTRIBUTE_CATEGORY,
                CASE WHEN PH.ATTRIBUTE_CATEGORY LIKE 'LS %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'LS FR OU E%' THEN PH.ATTRIBUTE1 ELSE NULL END AS PH_DFF_SAMPLE_PO,
                CASE WHEN PH.ATTRIBUTE_CATEGORY LIKE 'LS %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'LS FR OU E%' THEN PH.ATTRIBUTE2 ELSE NULL END AS PH_DFF_CPA_NUMBER,
                CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS%' THEN PH.ATTRIBUTE4 ELSE NULL END AS PH_DFF_PRINTED_REVISION,
                CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE5 ELSE NULL END AS PH_DFF_MANUFACTURER,
                CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE6 ELSE NULL END AS PH_DFF_PO_ACCEPTANCE_NO,
                CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE10 ELSE NULL END AS PH_DFF_PRINT_DATE,
                CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE11 ELSE NULL END AS PH_DFF_PRINT_EMAIL,
                CASE WHEN PH.ATTRIBUTE_CATEGORY LIKE 'LS %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'LS FR OU E%' THEN PH.ATTRIBUTE12 ELSE NULL END AS PH_DFF_ASSET_FLAG,
                CASE WHEN PH.ATTRIBUTE_CATEGORY LIKE 'LS %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'LS UK%' THEN PH.ATTRIBUTE13 ELSE NULL END AS PH_DFF_ITEM_REVISION,
                CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE14 ELSE NULL END AS PH_DFF_STOP_TRANSMIT,
                CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE15 ELSE NULL END AS PH_DFF_PLANNER,
                PH.CLOSED_CODE       PH_CLOSED_CODE,
                PH.ORG_ID       PH_ORG_ID,
                odt.HOU_NAME OU_NAME,
                PH.WF_ITEM_TYPE       PH_WF_ITEM_TYPE,
                PH.WF_ITEM_KEY       PH_WF_ITEM_KEY,
                PH.CHANGE_SUMMARY       PH_CHANGE_SUMMARY,
                PH.DOCUMENT_CREATION_METHOD       PH_DOCUMENT_CREATION_METHOD,
                PH.SUBMIT_DATE       PH_SUBMIT_DATE,
                PH.SUPPLIER_NOTIF_METHOD       PH_SUPPLIER_NOTIF_METHOD,
                PH.EMAIL_ADDRESS       PH_EMAIL_ADDRESS,
                PH.CLM_DOCUMENT_NUMBER       PH_CLM_DOCUMENT_NUMBER,
                PH.CLM_EFFECTIVE_DATE       PH_CLM_EFFECTIVE_DATE,
                PL.PO_LINE_ID       PL_PO_LINE_ID,
                PL.LAST_UPDATE_DATE       PL_LAST_UPDATE_DATE,
                FUPLB.user_name      PL_LAST_UPDATED_BY,
                PL.LINE_NUM       PL_LINE_NUM,
                PL.CREATION_DATE       PL_CREATION_DATE,
                FUPL.user_name       PL_CREATED_BY,
                to_char(PL.ITEM_ID)       PL_ITEM_ID,
                PL.CATEGORY_ID       PL_CATEGORY_ID,
                PL.ITEM_DESCRIPTION       PL_ITEM_DESCRIPTION,
                PL.UNIT_MEAS_LOOKUP_CODE       PL_UNIT_MEAS_LOOKUP_CODE,
                PL.QUANTITY_COMMITTED       PL_QUANTITY_COMMITTED,
                PL.COMMITTED_AMOUNT       PL_COMMITTED_AMOUNT,
                PL.ALLOW_PRICE_OVERRIDE_FLAG       PL_ALLOW_PRICE_OVERRIDE_FLAG,
                PL.LIST_PRICE_PER_UNIT       PL_LIST_PRICE_PER_UNIT,
                PL.UNIT_PRICE       PL_UNIT_PRICE,
                PL.NOTE_TO_VENDOR       PL_NOTE_TO_VENDOR,
                PL.CLOSED_FLAG       PL_CLOSED_FLAG,
                PL.CANCEL_FLAG       PL_CANCEL_FLAG,
                fuplc.user_name       PL_CANCELLED_BY,
                PL.CANCEL_DATE       PL_CANCEL_DATE,
                PL.CANCEL_REASON       PL_CANCEL_REASON,
                PL.VENDOR_PRODUCT_NUM       PL_VENDOR_PRODUCT_NUM,
                PL.ATTRIBUTE_CATEGORY       PL_ATTRIBUTE_CATEGORY,
                CASE when PL.ATTRIBUTE_CATEGORY LIKE 'LS %' then pl.attribute2 else NULL end as PL_DFF_ADVANCED_PRICING,
                CASE when PL.ATTRIBUTE_CATEGORY LIKE 'LS %' then pl.attribute3 else NULL end as PL_DFF_FAF_PRICE, 
                CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PL.ATTRIBUTE4 ELSE NULL END AS PL_DFF_PACKAGING_OPTION_ID,
                CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PL.ATTRIBUTE6 ELSE NULL END AS PL_DFF_REASON,
                CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PL.ATTRIBUTE7 ELSE NULL END AS PL_DFF_BPA_LEADTIME,
                CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' AND  length(PL.ATTRIBUTE11) = 10 THEN TO_DATE(SUBSTR(PL.ATTRIBUTE11, 0, 10), 'YYYY/MM/DD') ELSE NULL END AS PL_DFF_INCOTERM_PROMISE_DATE,
                CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' AND  length(PL.ATTRIBUTE12) = 10 THEN TO_DATE(SUBSTR(PL.ATTRIBUTE12, 0, 10), 'YYYY/MM/DD') ELSE NULL END AS PL_DFF_INCOTERM_DUE_DATE,
                CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PL.ATTRIBUTE13 ELSE NULL END AS PL_DFF_PO_ACCEPTANCE_NO,
                PL.PRICE_TYPE_LOOKUP_CODE       PL_PRICE_TYPE_LOOKUP_CODE,
                PL.CLOSED_CODE       PL_CLOSED_CODE,
                PL.PRICE_BREAK_LOOKUP_CODE       PL_PRICE_BREAK_LOOKUP_CODE,
                PL.CLOSED_DATE       PL_CLOSED_DATE,
                PL.CLOSED_REASON       PL_CLOSED_REASON,
                fuplcl.user_name      PL_CLOSED_BY,
                PL.EXPIRATION_DATE       PL_EXPIRATION_DATE,
                PL.CONTRACT_ID       PL_CONTRACT_ID,
                PL.BASE_UNIT_PRICE       PL_BASE_UNIT_PRICE,
                PL.MANUAL_PRICE_CHANGE_FLAG       PL_MANUAL_PRICE_CHANGE_FLAG,
                PR.PO_RELEASE_ID       PR_PO_RELEASE_ID,
                PR.LAST_UPDATE_DATE       PR_LAST_UPDATE_DATE,
                FUPRB.user_name       PR_LAST_UPDATED_BY,
                PR.RELEASE_NUM       PR_RELEASE_NUM,
                PR.AGENT_ID       PR_AGENT_ID,
                PR.RELEASE_DATE       PR_RELEASE_DATE,
                PR.CREATION_DATE       PR_CREATION_DATE,
                FUPR.user_name       PR_CREATED_BY,
                PR.REVISION_NUM       PR_REVISION_NUM,
                PR.REVISED_DATE       PR_REVISED_DATE,
                PR.APPROVED_FLAG       PR_APPROVED_FLAG,
                PR.APPROVED_DATE       PR_APPROVED_DATE,
                PR.PRINT_COUNT       PR_PRINT_COUNT,
                PR.PRINTED_DATE       PR_PRINTED_DATE,
                PR.ACCEPTANCE_REQUIRED_FLAG       PR_ACCEPTANCE_REQUIRED_FLAG,
                fuprh.user_name       PR_HOLD_BY,
                PR.HOLD_DATE       PR_HOLD_DATE,
                PR.HOLD_REASON       PR_HOLD_REASON,
                PR.HOLD_FLAG       PR_HOLD_FLAG,
                PR.CANCEL_FLAG       PR_CANCEL_FLAG,
                fuprc.user_name       PR_CANCELLED_BY,
                PR.CANCEL_DATE       PR_CANCEL_DATE,
                PR.CANCEL_REASON       PR_CANCEL_REASON,
                PR.ATTRIBUTE_CATEGORY       PR_ATTRIBUTE_CATEGORY,
                CASE WHEN PR.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PR.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PR.ATTRIBUTE4 ELSE NULL END AS PR_DFF_PRINTED_REVISION,
                CASE WHEN PR.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PR.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PR.ATTRIBUTE6 ELSE NULL END AS PR_DFF_PO_ACCEPTANCE_NO,
                CASE WHEN PR.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PR.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PR.ATTRIBUTE7 ELSE NULL END AS PR_DFF_PRINT_DATE,
                CASE WHEN PR.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PR.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PR.ATTRIBUTE8 ELSE NULL END AS PR_DFF_PRINT_MAIL,
                CASE WHEN PR.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PR.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PR.ATTRIBUTE9 ELSE NULL END AS PR_DFF_PLANNER,
                CASE WHEN PR.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PR.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PR.ATTRIBUTE14 ELSE NULL END AS PR_DFF_STOP_TRANSMIT,
                PR.AUTHORIZATION_STATUS       PR_AUTHORIZATION_STATUS,
                PR.CLOSED_CODE       PR_CLOSED_CODE,
                PR.RELEASE_TYPE       PR_RELEASE_TYPE,
                PR.NOTE_TO_VENDOR       PR_NOTE_TO_VENDOR,
                PR.WF_ITEM_TYPE       PR_WF_ITEM_TYPE,
                PR.WF_ITEM_KEY       PR_WF_ITEM_KEY,
                PR.VENDOR_ORDER_NUM       PR_VENDOR_ORDER_NUM,
                PR.DOCUMENT_CREATION_METHOD       PR_DOCUMENT_CREATION_METHOD,
                PR.SUBMIT_DATE       PR_SUBMIT_DATE,
                PLL.LINE_LOCATION_ID       PLL_LINE_LOCATION_ID,
                PLL.LAST_UPDATE_DATE       PLL_LAST_UPDATE_DATE,
                FUPLLB.user_name       PLL_LAST_UPDATED_BY,
                PLL.CREATION_DATE       PLL_CREATION_DATE,
                FUPLL.user_name       PLL_CREATED_BY,
                PLL.QUANTITY       PLL_QUANTITY,
                PLL.QUANTITY_RECEIVED       PLL_QUANTITY_RECEIVED,
                PLL.QUANTITY_ACCEPTED       PLL_QUANTITY_ACCEPTED,
                PLL.QUANTITY_REJECTED       PLL_QUANTITY_REJECTED,
                PLL.QUANTITY_BILLED       PLL_QUANTITY_BILLED,
                PLL.QUANTITY_CANCELLED       PLL_QUANTITY_CANCELLED,
                PLL.UNIT_MEAS_LOOKUP_CODE       PLL_UNIT_MEAS_LOOKUP_CODE,
                PLL.SHIP_TO_LOCATION_ID       PLL_SHIP_TO_LOCATION_ID,
                PLL.NEED_BY_DATE       PLL_NEED_BY_DATE,
                PLL.PROMISED_DATE       PLL_PROMISED_DATE,
                PLL.LAST_ACCEPT_DATE       PLL_LAST_ACCEPT_DATE,
                PLL.PRICE_OVERRIDE       PLL_PRICE_OVERRIDE,
                PLL.TAXABLE_FLAG       PLL_TAXABLE_FLAG,
                PLL.FROM_HEADER_ID       PLL_FROM_HEADER_ID,
                PLL.FROM_LINE_ID       PLL_FROM_LINE_ID,
                PLL.FROM_LINE_LOCATION_ID       PLL_FROM_LINE_LOCATION_ID,
                PLL.APPROVED_FLAG       PLL_APPROVED_FLAG,
                PLL.APPROVED_DATE       PLL_APPROVED_DATE,
                PLL.CLOSED_FLAG       PLL_CLOSED_FLAG,
                PLL.CANCEL_FLAG       PLL_CANCEL_FLAG,
                fupllc.user_name       PLL_CANCELLED_BY,
                PLL.CANCEL_DATE       PLL_CANCEL_DATE,
                PLL.CANCEL_REASON       PLL_CANCEL_REASON,
                PLL.ATTRIBUTE_CATEGORY       PLL_ATTRIBUTE_CATEGORY,
                CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PLL.ATTRIBUTE10 ELSE NULL END AS PLL_DFF_PACKAGING_OPTION_ID,
                CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PLL.ATTRIBUTE11 ELSE NULL END AS PLL_DFF_REASON,
                CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' AND  length(PLL.ATTRIBUTE12) = 10 THEN TO_DATE(SUBSTR(PLL.ATTRIBUTE12, 0, 10), 'YYYY/MM/DD') ELSE NULL END AS PLL_DFF_INCOTERM_PROMISE_DATE,
                CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PLL.ATTRIBUTE13 ELSE NULL END AS PLL_DFF_FAF_PRICE,
                CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' AND  length(PLL.ATTRIBUTE14) = 10 THEN TO_DATE(SUBSTR(PLL.ATTRIBUTE14, 0, 10), 'YYYY/MM/DD') ELSE NULL END AS PLL_DFF_INCOTERM_DUE_DATE,
                CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PLL.ATTRIBUTE15 ELSE NULL END AS PLL_DFF_PO_ACCEPTANCE_NO,
                PLL.QTY_RCV_TOLERANCE       PLL_QTY_RCV_TOLERANCE,
                PLL.QTY_RCV_EXCEPTION_CODE       PLL_QTY_RCV_EXCEPTION_CODE,
                PLL.ENFORCE_SHIP_TO_LOCATION_CODE       PLL_ENFORCE_SHIP_TO_LOCATION_CODE,
                PLL.DAYS_EARLY_RECEIPT_ALLOWED       PLL_DAYS_EARLY_RECEIPT_ALLOWED,
                PLL.DAYS_LATE_RECEIPT_ALLOWED       PLL_DAYS_LATE_RECEIPT_ALLOWED,
                PLL.RECEIPT_DAYS_EXCEPTION_CODE       PLL_RECEIPT_DAYS_EXCEPTION_CODE,
                PLL.RECEIVE_CLOSE_TOLERANCE       PLL_RECEIVE_CLOSE_TOLERANCE,
                PLL.SHIP_TO_ORGANIZATION_ID       PLL_SHIP_TO_ORGANIZATION_ID,
                idt.OOD_ORGANIZATION_CODE       PLL_IO,
                PLL.SHIPMENT_NUM       PLL_SHIPMENT_NUM,
                PLL.SHIPMENT_TYPE       PLL_SHIPMENT_TYPE,
                PLL.CLOSED_CODE       PLL_CLOSED_CODE,
                PLL.CLOSED_REASON       PLL_CLOSED_REASON,
                PLL.CLOSED_DATE       PLL_CLOSED_DATE,
                fupllcl.user_name       PLL_CLOSED_BY,
                PLL.COUNTRY_OF_ORIGIN_CODE       PLL_COUNTRY_OF_ORIGIN_CODE,
                PLL.MATCH_OPTION       PLL_MATCH_OPTION,
                PLL.NOTE_TO_RECEIVER       PLL_NOTE_TO_RECEIVER,
                PLL.VMI_FLAG       PLL_VMI_FLAG,
                PLL.CONSIGNED_FLAG       PLL_CONSIGNED_FLAG,
                PLL.AMOUNT_BILLED       PLL_AMOUNT_BILLED,
                PLL.DROP_SHIP_FLAG       PLL_DROP_SHIP_FLAG,
                PLL.MANUAL_PRICE_CHANGE_FLAG       PLL_MANUAL_PRICE_CHANGE_FLAG,
                PLL.SHIPMENT_CLOSED_DATE       PLL_SHIPMENT_CLOSED_DATE,
                PLL.CLOSED_FOR_RECEIVING_DATE       PLL_CLOSED_FOR_RECEIVING_DATE,
                PLL.CLOSED_FOR_INVOICE_DATE       PLL_CLOSED_FOR_INVOICE_DATE,
                PDA.PO_DISTRIBUTION_ID       PDA_PO_DISTRIBUTION_ID,
                PDA.LAST_UPDATE_DATE       PDA_LAST_UPDATE_DATE,
                FUPDAB.user_name       PDA_LAST_UPDATED_BY,
                PDA.SET_OF_BOOKS_ID       PDA_SET_OF_BOOKS_ID,
                PDA.CODE_COMBINATION_ID       PDA_CODE_COMBINATION_ID,
                PDA.QUANTITY_ORDERED       PDA_QUANTITY_ORDERED,
                PDA.CREATION_DATE       PDA_CREATION_DATE,
                FUPDA.user_name       PDA_CREATED_BY,
                PDA.QUANTITY_DELIVERED       PDA_QUANTITY_DELIVERED,
                PDA.QUANTITY_BILLED       PDA_QUANTITY_BILLED,
                PDA.QUANTITY_CANCELLED       PDA_QUANTITY_CANCELLED,
                PDA.REQ_HEADER_REFERENCE_NUM       PDA_REQ_HEADER_REFERENCE_NUM,
                PDA.REQ_LINE_REFERENCE_NUM       PDA_REQ_LINE_REFERENCE_NUM,
                PDA.REQ_DISTRIBUTION_ID       PDA_REQ_DISTRIBUTION_ID,
                PDA.DELIVER_TO_LOCATION_ID       PDA_DELIVER_TO_LOCATION_ID,
                PDA.DELIVER_TO_PERSON_ID       PDA_DELIVER_TO_PERSON_ID,
                PDA.RATE_DATE       PDA_RATE_DATE,
                PDA.RATE       PDA_RATE,
                PDA.AMOUNT_BILLED       PDA_AMOUNT_BILLED,
                PDA.ACCRUED_FLAG       PDA_ACCRUED_FLAG,
                PDA.GL_CANCELLED_DATE       PDA_GL_CANCELLED_DATE,
                PDA.DESTINATION_TYPE_CODE       PDA_DESTINATION_TYPE_CODE,
                PDA.DESTINATION_ORGANIZATION_ID       PDA_DESTINATION_ORGANIZATION_ID,
                PDA.ATTRIBUTE_CATEGORY       PDA_ATTRIBUTE_CATEGORY,
                CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE1 ELSE NULL END AS PDA_DFF_COUNTRY,
                CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE2 ELSE NULL END AS PDA_DFF_STATE,
                CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE3 ELSE NULL END AS PDA_DFF_COUNTY,
                CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE4 ELSE NULL END AS PDA_DFF_CITY,
                CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE5 ELSE NULL END AS PDA_DFF_SITE,
                CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE6 ELSE NULL END AS PDA_DFF_DEPT,
                CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE9 ELSE NULL END AS PDA_DFF_RSN_ASSET_CREATION,
                CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE10 ELSE NULL END AS PDA_DFF_GPAO_PROD_ORDER_NUMBER,
                CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE11 ELSE NULL END AS PDA_DFF_PRODUCT_GROUP,
                PDA.ATTRIBUTE15       DFF_AR_NUMBER,
                PDA.ACCRUAL_ACCOUNT_ID       PDA_ACCRUAL_ACCOUNT_ID,
                PDA.VARIANCE_ACCOUNT_ID       PDA_VARIANCE_ACCOUNT_ID,
                PDA.DESTINATION_CONTEXT       PDA_DESTINATION_CONTEXT,
                PDA.DISTRIBUTION_NUM       PDA_DISTRIBUTION_NUM,
                PDA.PROJECT_ACCOUNTING_CONTEXT       PDA_PROJECT_ACCOUNTING_CONTEXT,
                PDA.TAX_RECOVERY_OVERRIDE_FLAG       PDA_TAX_RECOVERY_OVERRIDE_FLAG,
                PDA.DISTRIBUTION_TYPE       PDA_DISTRIBUTION_TYPE,
                ROWNUM AS ROW_NUMBER_ID,  
                SYSDATE AS ROW_CREATION_DATE,  
                SYSDATE AS ROW_LAST_UPDATE_DATE,
                gcck.SEGMENT1 PO_ACCOUNT_BU_GL_SGT1,
                gcck.SEGMENT2 PO_ACCOUNT_LOCATION_GL_SGT2,
                gcck.SEGMENT3 PO_ACCOUNT_DEPARTMENT_GL_SGT3,
                gcck.SEGMENT4 PO_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                gcck.SEGMENT5 PO_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                gcck.SEGMENT6 PO_ACCOUNT_INTERCOMPANY_GL_SGT6,
                sgt5.PRODUCT_GROUP_GL_DESCRIPTION,
                hla.LOCATION_CODE SHIP_TO_LOCATION_CODE,
                hla2.LOCATION_CODE BILL_TO_LOCATION_CODE,
                decode(sgt5.PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT, null, sgt2.LOCATION_GL_BU_VENTE, fvb.VALEUR)   as BU_NEW,
                odt.GSOB_CURRENCY_CODE as OU_CURRENCY,
                qr.character6 LEAD_TIME_TO_NAMED_PLACE_LS  --> qr.character6 : mis en commentaire car cela remontre trop de doublon , cas � �tudier
            FROM steph_apps_po_distributions_all_bz pda
            LEFT JOIN steph_apps_po_line_locations_all_bz pll ON pda.line_location_id = pll.line_location_id
            LEFT JOIN steph_apps_po_releases_all_bz pr on pll.po_release_id  = pr.po_release_id 
            LEFT JOIN steph_apps_po_lines_all_bz pl ON pll.po_line_id  = pl.po_line_id
            LEFT JOIN steph_apps_po_headers_all_bz ph on pl.po_header_id =  ph.po_header_id  
            LEFT JOIN STEPH_APPS_FND_USER_bz fupr ON pr.CREATED_BY = fupr.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuprb ON pr.LAST_UPDATED_BY = fuprb.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuprh ON pr.HOLD_BY = fuprh.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuprc ON pr.CANCELLED_BY = fuprc.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuph ON ph.CREATED_BY = fuph.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuphb ON ph.LAST_UPDATED_BY = fuphb.user_ID
            LEFT JOIN STEPH_APPS_AP_TERMS_BZ aat ON PH.TERMS_ID = aat.term_id
            LEFT JOIN OU_DETAILS_TEMP odt ON PH.ORG_ID = odt.HOU_ORGANIZATION_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fupl ON pl.CREATED_BY = fupl.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuplb ON pl.LAST_UPDATED_BY = fuplb.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuplc ON pl.CANCELLED_BY = fuplc.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuplcl ON pl.CLOSED_BY = fuplcl.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fupll ON pll.CREATED_BY = fupll.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fupllb ON pll.LAST_UPDATED_BY = fupllb.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fupllc ON pll.CANCELLED_BY = fupllc.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fupllcl ON pll.CLOSED_BY = fupllcl.user_ID
            LEFT JOIN IO_DETAILS_TEMP idt ON pll.SHIP_TO_ORGANIZATION_ID = idt.OOD_ORGANIZATION_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fupda ON pda.CREATED_BY = fupda.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fupdab ON pda.LAST_UPDATED_BY = fupdab.user_ID
            LEFT JOIN STEPH_APPS_GL_CODE_COMBINATIONS_KFV_BZ gcck on PDA.CODE_COMBINATION_ID = gcck.CODE_COMBINATION_ID
            LEFT JOIN STEPH_APPS_HR_LOCATIONS_ALL_BZ HLA on pll.ship_to_location_id = hla.location_id
            LEFT JOIN STEPH_APPS_HR_LOCATIONS_ALL_BZ HLA2 on PH.BILL_TO_LOCATION_ID = hla2.location_id
            LEFT JOIN FILE_PRODUCTGROUP_SEGMENT5_BZ sgt5 on nvl(pda.aTtribute11,gcck.SEGMENT5) = sgt5.product_group_gl_code
            /*  LEFT JOIN FILE_VAR_BU_BZ fvb on sgt5.PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT = fvb.subbu
          remplacer par la jointure avec FILE_TRANSFORMATION_DATA*/
            LEFT JOIN FILE_TRANSFORMATION_DATA_BZ fvb
                     ON fvb.cle = sgt5.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE
                     AND fvb.type = 'BU_NEW'
            LEFT JOIN FILE_LOCATION_SEGMENT2_BZ sgt2 on gcck.SEGMENT2 = sgt2.location_gl_code
   /* mis en commentaire car cela remontre trop de doublon , cas � �tudier */
   /*     LEFT JOIN steph_apps_qa_results_bz qr ON
                qr.item_id = to_char(PL.ITEM_ID)
                AND qr.vendor_id = PH.VENDOR_ID
                AND qr.character5 = PH.FOB_LOOKUP_CODE
                AND qr.character1 = SUBSTR(ph.FREIGHT_TERMS_LOOKUP_CODE, -3)
                and qr.plan_id = ( select qp.plan_id from steph_apps_qa_plans_bz qp where qp.name = 'LEAD TIME TO NAMED PLACE LS'  )
     */
       LEFT JOIN (
              SELECT *
              FROM (
                    SELECT 
                        qr.*, 
                       ROW_NUMBER() OVER (PARTITION BY item_id, vendor_id, character5, character1, plan_id ORDER BY qr.last_update_date DESC) rn
                       FROM steph_apps_qa_results_bz qr
                       WHERE qr.plan_id = (
                  SELECT qp.plan_id 
                  FROM steph_apps_qa_plans_bz qp 
                  WHERE qp.name LIKE '%LEAD TIME TO NAMED PLACE LS%'
                )
              )
              WHERE rn = 1
            ) qr ON 
              qr.item_id = TO_CHAR(PL.ITEM_ID)
              AND qr.vendor_id = PH.VENDOR_ID
              AND qr.character5 = PH.FOB_LOOKUP_CODE
              AND qr.character1 = SUBSTR(PH.FREIGHT_TERMS_LOOKUP_CODE, -3)
            WHERE  1 = 1
                and ph.type_lookup_code = 'BLANKET'            

;

  g_status   := 'COMPLETED';
     g_etape    := '010 - INSERT INTO' ;
     Write_Log_PROC;


     g_table     := 'PO_BLANKET'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('PO_BLANKET'); 
     g_status   := 'COMPLETED';
     g_etape    := '099 - STATS' ; 
     Write_Log_PROC;

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
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

   END PO_BLANKET_PROC;
  
    /****************************************************************************************
    * PROCEDURE   :  PO_STANDARD_PROC
    * DESCRIPTION :  Create PO_BLANKET TABLE
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
   PROCEDURE PO_STANDARD_PROC
   IS   v_procedure varchar2(100) := 'PO_STANDARD_PROC';
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

     g_table     := 'PO_STANDARD'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE PO_STANDARD';
     g_status   := 'COMPLETED';
     g_etape    := '011 - TRUNCATE TABLE' ;
     Write_Log_PROC;

     g_table     := 'PO_STANDARD'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'   ;
        INSERT /*+ APPEND PARALLEL(PO_STANDARD, 8) MONITOR gather_plan_statistics*/
        INTO  PO_STANDARD
                              select /*+
                      PARALLEL(pda 6)
                      PARALLEL(pll 6)
                      PARALLEL(pl 6)
                      PARALLEL(ph 6)
                      PARALLEL(fupr 6)
                      PARALLEL(fuprb 6)
                      PARALLEL(fuprh 6)
                      PARALLEL(fuprc 6)
                      PARALLEL(fuph 6)
                      PARALLEL(fuphb 6)
                      PARALLEL(aat 6)
                      PARALLEL(odt 6)
                      PARALLEL(fupl 6)
                      PARALLEL(fuplb 6)
                      PARALLEL(fuplc 6)
                      PARALLEL(fuplcl 6)
                      PARALLEL(fuplcl 6)
                      PARALLEL(fupll 6)
                      PARALLEL(fupllb 6)
                      PARALLEL(fupllc 6)
                      PARALLEL(idt 6)
                      PARALLEL(fupda 6)
                      PARALLEL(fupdab 6)
                      PARALLEL(gcck 6)
                      PARALLEL(HLA 6)
                      PARALLEL(HLA2 6)
                      PARALLEL(sgt5 6)
                      PARALLEL(fvb 6)
                      PARALLEL(sgt2 6)
        gather_plan_statistics
    */

            PH.PO_HEADER_ID PH_PO_HEADER_ID,
            PH.AGENT_ID PH_AGENT_ID,
            LH2_DTH_SILVER_FUNCTIONS_PKG.GET_STEPH_APPS_PER_ALL_PEOPLE_F_NAME_FUNC(ph.agent_id) PO_BUYER,
            PH.TYPE_LOOKUP_CODE PH_TYPE_LOOKUP_CODE,
            PH.LAST_UPDATE_DATE PH_LAST_UPDATE_DATE,
            fuphb.user_name PH_LAST_UPDATED_BY,
            PH.SEGMENT1 PO_NUMBER,
            PH.CREATION_DATE PH_CREATION_DATE,
            fuph.user_name PH_CREATED_BY,
            PH.VENDOR_ID PH_VENDOR_ID,
            PH.VENDOR_SITE_ID PH_VENDOR_SITE_ID,
            PH.VENDOR_CONTACT_ID PH_VENDOR_CONTACT_ID,
            PH.SHIP_TO_LOCATION_ID PH_SHIP_TO_LOCATION_ID,
            PH.BILL_TO_LOCATION_ID PH_BILL_TO_LOCATION_ID,
            PH.TERMS_ID PH_TERMS_ID,
            aat.name       PH_TERMS_NAME,
            PH.SHIP_VIA_LOOKUP_CODE PH_SHIP_VIA_LOOKUP_CODE,
            PH.FOB_LOOKUP_CODE PH_FOB_LOOKUP_CODE,
            PH.FREIGHT_TERMS_LOOKUP_CODE PH_FREIGHT_TERMS_LOOKUP_CODE,
            PH.CURRENCY_CODE PH_CURRENCY_CODE,
            PH.RATE_TYPE PH_RATE_TYPE,
            PH.RATE_DATE PH_RATE_DATE,
            PH.RATE PH_RATE,
            PH.FROM_HEADER_ID PH_FROM_HEADER_ID,
            PH.FROM_TYPE_LOOKUP_CODE PH_FROM_TYPE_LOOKUP_CODE,
            PH.AUTHORIZATION_STATUS PH_AUTHORIZATION_STATUS,
            PH.REVISION_NUM PH_REVISION_NUM,
            PH.REVISED_DATE PH_REVISED_DATE,
            PH.APPROVED_FLAG PH_APPROVED_FLAG,
            PH.APPROVED_DATE PH_APPROVED_DATE,
            PH.NOTE_TO_VENDOR PH_NOTE_TO_VENDOR,
            PH.NOTE_TO_RECEIVER PH_NOTE_TO_RECEIVER,
            PH.PRINT_COUNT PH_PRINT_COUNT,
            PH.PRINTED_DATE PH_PRINTED_DATE,
            PH.VENDOR_ORDER_NUM PH_VENDOR_ORDER_NUM,
            PH.CONFIRMING_ORDER_FLAG PH_CONFIRMING_ORDER_FLAG,
            PH.COMMENTS PH_COMMENTS,
            PH.ACCEPTANCE_REQUIRED_FLAG PH_ACCEPTANCE_REQUIRED_FLAG,
            PH.ACCEPTANCE_DUE_DATE PH_ACCEPTANCE_DUE_DATE,
            PH.CLOSED_DATE PH_CLOSED_DATE,
            PH.USER_HOLD_FLAG PH_USER_HOLD_FLAG,
            PH.APPROVAL_REQUIRED_FLAG PH_APPROVAL_REQUIRED_FLAG,
            PH.CANCEL_FLAG PH_CANCEL_FLAG,
            PH.SUPPLY_AGREEMENT_FLAG PH_SUPPLY_AGREEMENT_FLAG,
            PH.ATTRIBUTE_CATEGORY PH_ATTRIBUTE_CATEGORY,
            CASE WHEN PH.ATTRIBUTE_CATEGORY LIKE 'LS %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'LS FR OU E%' THEN PH.ATTRIBUTE1 ELSE NULL END AS PH_DFF_SAMPLE_PO,
            CASE WHEN PH.ATTRIBUTE_CATEGORY LIKE 'LS %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'LS FR OU E%' THEN PH.ATTRIBUTE2 ELSE NULL END AS PH_DFF_CPA_NUMBER,
            CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS%' THEN PH.ATTRIBUTE4 ELSE NULL END AS PH_DFF_PRINTED_REVISION,
            CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE5 ELSE NULL END AS PH_DFF_MANUFACTURER,
            CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE6 ELSE NULL END AS PH_DFF_PO_ACCEPTANCE_NO,
            CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE10 ELSE NULL END AS PH_DFF_PRINT_DATE,
            CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE11 ELSE NULL END AS PH_DFF_PRINT_EMAIL,
            CASE WHEN PH.ATTRIBUTE_CATEGORY LIKE 'LS %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'LS FR OU E%' THEN PH.ATTRIBUTE12 ELSE NULL END AS PH_DFF_ASSET_FLAG,
            CASE WHEN PH.ATTRIBUTE_CATEGORY LIKE 'LS %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'LS UK%' THEN PH.ATTRIBUTE13 ELSE NULL END AS PH_DFF_ITEM_REVISION,
            CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE14 ELSE NULL END AS PH_DFF_STOP_TRANSMIT,
            CASE WHEN PH.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PH.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PH.ATTRIBUTE15 ELSE NULL END AS PH_DFF_PLANNER,
            PH.CLOSED_CODE PH_CLOSED_CODE,
            PH.ORG_ID PH_ORG_ID,
            odt.HOU_NAME OU_NAME,
            PH.REFERENCE_NUM PH_REFERENCE_NUM,
            PH.WF_ITEM_TYPE PH_WF_ITEM_TYPE,
            PH.WF_ITEM_KEY PH_WF_ITEM_KEY,
            PH.XML_FLAG PH_XML_FLAG,
            PH.XML_SEND_DATE PH_XML_SEND_DATE,
            PH.XML_CHANGE_SEND_DATE PH_XML_CHANGE_SEND_DATE,
            ph.CHANGE_REQUESTED_BY PH_CHANGE_REQUESTED_BY,
            PH.SHIPPING_CONTROL PH_SHIPPING_CONTROL,
            PH.CONTERMS_EXIST_FLAG PH_CONTERMS_EXIST_FLAG,
            PH.PENDING_SIGNATURE_FLAG PH_PENDING_SIGNATURE_FLAG,
            PH.CHANGE_SUMMARY PH_CHANGE_SUMMARY,
            PH.DOCUMENT_CREATION_METHOD PH_DOCUMENT_CREATION_METHOD,
            PH.SUBMIT_DATE PH_SUBMIT_DATE,
            PH.SUPPLIER_NOTIF_METHOD PH_SUPPLIER_NOTIF_METHOD,
            PH.EMAIL_ADDRESS PH_EMAIL_ADDRESS,
            PH.LOCK_OWNER_ROLE PH_LOCK_OWNER_ROLE,
            PH.LOCK_OWNER_USER_ID PH_LOCK_OWNER_USER_ID,
            PH.CLM_DOCUMENT_NUMBER PH_CLM_DOCUMENT_NUMBER,
            PH.CLM_EFFECTIVE_DATE PH_CLM_EFFECTIVE_DATE,
            PL.PO_LINE_ID PL_PO_LINE_ID,
            PL.LAST_UPDATE_DATE PL_LAST_UPDATE_DATE,
            fuplb.user_name  PL_LAST_UPDATED_BY,
            PL.LINE_NUM PL_LINE_NUM,
            PL.CREATION_DATE PL_CREATION_DATE,
            fupl.user_name PL_CREATED_BY,
            to_char(PL.ITEM_ID) PL_ITEM_ID,
            PL.ITEM_REVISION PL_ITEM_REVISION,
            PL.CATEGORY_ID PL_CATEGORY_ID,
            PL.ITEM_DESCRIPTION PL_ITEM_DESCRIPTION,
            PL.UNIT_MEAS_LOOKUP_CODE PL_UNIT_MEAS_LOOKUP_CODE,
            PL.LIST_PRICE_PER_UNIT PL_LIST_PRICE_PER_UNIT,
            PL.UNIT_PRICE PL_UNIT_PRICE,
            PL.QUANTITY PL_QUANTITY,
            PL.NOTE_TO_VENDOR PL_NOTE_TO_VENDOR,
            PL.QTY_RCV_TOLERANCE PL_QTY_RCV_TOLERANCE,
            PL.OVER_TOLERANCE_ERROR_FLAG PL_OVER_TOLERANCE_ERROR_FLAG,
            PL.CLOSED_FLAG PL_CLOSED_FLAG,
            PL.CANCEL_FLAG PL_CANCEL_FLAG,
            fuplc.user_name PL_CANCELLED_BY,
            PL.CANCEL_DATE PL_CANCEL_DATE,
            PL.CANCEL_REASON PL_CANCEL_REASON,
            PL.VENDOR_PRODUCT_NUM PL_VENDOR_PRODUCT_NUM,
            PL.TYPE_1099 PL_TYPE_1099,
            pl.NEGOTIATED_BY_PREPARER_FLAG PL_NEGOTIATED_BY_PREPARER_FLAG,
            PL.ATTRIBUTE_CATEGORY PL_ATTRIBUTE_CATEGORY,
            CASE when PL.ATTRIBUTE_CATEGORY LIKE 'LS %' then pl.attribute2 else NULL end as PL_DFF_ADVANCED_PRICING,
            CASE when PL.ATTRIBUTE_CATEGORY LIKE 'LS %' then pl.attribute3 else NULL end as PL_DFF_FAF_PRICE, 
            CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PL.ATTRIBUTE4 ELSE NULL END AS PL_DFF_PACKAGING_OPTION_ID,
            CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PL.ATTRIBUTE6 ELSE NULL END AS PL_DFF_REASON,
            CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PL.ATTRIBUTE7 ELSE NULL END AS PL_DFF_BPA_LEADTIME,
            CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' AND  length(PL.ATTRIBUTE11) = 10 THEN SUBSTR(PL.ATTRIBUTE11, 0, 10) ELSE NULL END AS PL_DFF_INCOTERM_PROMISE_DATE,
            CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' AND  length(PL.ATTRIBUTE12) = 10 THEN SUBSTR(PL.ATTRIBUTE12, 0, 10) ELSE NULL END AS PL_DFF_INCOTERM_DUE_DATE,
            CASE WHEN PL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PL.ATTRIBUTE13 ELSE NULL END AS PL_DFF_PO_ACCEPTANCE_NO,
            PL.PRICE_TYPE_LOOKUP_CODE PL_PRICE_TYPE_LOOKUP_CODE,
            PL.CLOSED_CODE PL_CLOSED_CODE,
            PL.CLOSED_DATE PL_CLOSED_DATE,
            PL.CLOSED_REASON PL_CLOSED_REASON,
            fuplcl.user_name PL_CLOSED_BY,
            PL.RETROACTIVE_DATE PL_RETROACTIVE_DATE,
            PL.CONTRACT_ID PL_CONTRACT_ID,
            PL.AMOUNT PL_AMOUNT,
            PL.BASE_UNIT_PRICE PL_BASE_UNIT_PRICE,
            PL.MANUAL_PRICE_CHANGE_FLAG PL_MANUAL_PRICE_CHANGE_FLAG,
            PL.CLM_TOTAL_AMOUNT_ORDERED PL_CLM_TOTAL_AMOUNT_ORDERED,
            PLL.LINE_LOCATION_ID PLL_LINE_LOCATION_ID,
            PLL.LAST_UPDATE_DATE PLL_LAST_UPDATE_DATE,
            fupllb.user_name PLL_LAST_UPDATED_BY,
            PLL.CREATION_DATE PLL_CREATION_DATE,
            fupl.user_name PLL_CREATED_BY,
            PLL.QUANTITY PLL_QUANTITY,
            PLL.QUANTITY_RECEIVED PLL_QUANTITY_RECEIVED,
            PLL.QUANTITY_ACCEPTED PLL_QUANTITY_ACCEPTED,
            PLL.QUANTITY_REJECTED PLL_QUANTITY_REJECTED,
            PLL.QUANTITY_BILLED PLL_QUANTITY_BILLED,
            PLL.QUANTITY_CANCELLED PLL_QUANTITY_CANCELLED,
            PLL.UNIT_MEAS_LOOKUP_CODE PLL_UNIT_MEAS_LOOKUP_CODE,
            PLL.SHIP_TO_LOCATION_ID PLL_SHIP_TO_LOCATION_ID,
            PLL.NEED_BY_DATE PLL_NEED_BY_DATE,
            PLL.PROMISED_DATE PLL_PROMISED_DATE,
            PLL.LAST_ACCEPT_DATE PLL_LAST_ACCEPT_DATE,
            PLL.PRICE_OVERRIDE PLL_PRICE_OVERRIDE,
            PLL.TAXABLE_FLAG PLL_TAXABLE_FLAG,
            PLL.APPROVED_FLAG PLL_APPROVED_FLAG,
            PLL.APPROVED_DATE PLL_APPROVED_DATE,
            PLL.CLOSED_FLAG PLL_CLOSED_FLAG,
            PLL.CANCEL_FLAG PLL_CANCEL_FLAG,
            fupllc.user_name PLL_CANCELLED_BY,
            PLL.CANCEL_DATE PLL_CANCEL_DATE,
            PLL.CANCEL_REASON PLL_CANCEL_REASON,
            PLL.ATTRIBUTE_CATEGORY PLL_ATTRIBUTE_CATEGORY,
            CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PLL.ATTRIBUTE10 ELSE NULL END AS PLL_DFF_PACKAGING_OPTION_ID,
            CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PLL.ATTRIBUTE11 ELSE NULL END AS PLL_DFF_REASON,
            CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN SUBSTR(PLL.ATTRIBUTE12, 0, 10) ELSE NULL END AS PLL_DFF_INCOTERM_PROMISE_DATE,
            CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PLL.ATTRIBUTE13 ELSE NULL END AS PLL_DFF_FAF_PRICE,
            CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN SUBSTR(PLL.ATTRIBUTE14, 0, 10) ELSE NULL END AS PLL_DFF_INCOTERM_DUE_DATE,
            CASE WHEN PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PLL.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PLL.ATTRIBUTE15 ELSE NULL END AS PLL_DFF_PO_ACCEPTANCE_NO,
            PLL.INSPECTION_REQUIRED_FLAG PLL_INSPECTION_REQUIRED_FLAG,
            PLL.RECEIPT_REQUIRED_FLAG PLL_RECEIPT_REQUIRED_FLAG,
            PLL.QTY_RCV_TOLERANCE PLL_QTY_RCV_TOLERANCE,
            PLL.QTY_RCV_EXCEPTION_CODE PLL_QTY_RCV_EXCEPTION_CODE,
            PLL.ENFORCE_SHIP_TO_LOCATION_CODE PLL_ENFORCE_SHIP_TO_LOCATION_CODE,
            PLL.ALLOW_SUBSTITUTE_RECEIPTS_FLAG PLL_ALLOW_SUBSTITUTE_RECEIPTS_FLAG,
            PLL.DAYS_EARLY_RECEIPT_ALLOWED PLL_DAYS_EARLY_RECEIPT_ALLOWED,
            PLL.DAYS_LATE_RECEIPT_ALLOWED PLL_DAYS_LATE_RECEIPT_ALLOWED,
            PLL.RECEIPT_DAYS_EXCEPTION_CODE PLL_RECEIPT_DAYS_EXCEPTION_CODE,
            PLL.INVOICE_CLOSE_TOLERANCE PLL_INVOICE_CLOSE_TOLERANCE,
            PLL.RECEIVE_CLOSE_TOLERANCE PLL_RECEIVE_CLOSE_TOLERANCE,
            PLL.SHIP_TO_ORGANIZATION_ID PLL_SHIP_TO_ORGANIZATION_ID,
            idt.OOD_ORGANIZATION_CODE PLL_IO,
            PLL.SHIPMENT_NUM PLL_SHIPMENT_NUM,
            PLL.SHIPMENT_TYPE PLL_SHIPMENT_TYPE,
            PLL.CLOSED_CODE PLL_CLOSED_CODE,
            PLL.ACCRUE_ON_RECEIPT_FLAG PLL_ACCRUE_ON_RECEIPT_FLAG,
            PLL.CLOSED_REASON PLL_CLOSED_REASON,
            PLL.CLOSED_DATE PLL_CLOSED_DATE,
            fupllcl.user_name PLL_CLOSED_BY,
            PLL.QUANTITY_SHIPPED PLL_QUANTITY_SHIPPED,
            PLL.COUNTRY_OF_ORIGIN_CODE PLL_COUNTRY_OF_ORIGIN_CODE,
            PLL.TAX_USER_OVERRIDE_FLAG PLL_TAX_USER_OVERRIDE_FLAG,
            PLL.MATCH_OPTION PLL_MATCH_OPTION,
            PLL.CALCULATE_TAX_FLAG PLL_CALCULATE_TAX_FLAG,
            PLL.NOTE_TO_RECEIVER PLL_NOTE_TO_RECEIVER,
            PLL.VMI_FLAG PLL_VMI_FLAG,
            PLL.CONSIGNED_FLAG PLL_CONSIGNED_FLAG,
            PLL.SUPPLIER_ORDER_LINE_NUMBER PLL_SUPPLIER_ORDER_LINE_NUMBER,
            PLL.AMOUNT_BILLED PLL_AMOUNT_BILLED,
            PLL.DROP_SHIP_FLAG PLL_DROP_SHIP_FLAG,
            PLL.SHIPMENT_CLOSED_DATE PLL_SHIPMENT_CLOSED_DATE,
            PLL.CLOSED_FOR_RECEIVING_DATE PLL_CLOSED_FOR_RECEIVING_DATE,
            PLL.CLOSED_FOR_INVOICE_DATE PLL_CLOSED_FOR_INVOICE_DATE,
            PDA.PO_DISTRIBUTION_ID PDA_PO_DISTRIBUTION_ID,
            PDA.LAST_UPDATE_DATE PDA_LAST_UPDATE_DATE,
            fupdab.user_name PDA_LAST_UPDATED_BY,
            PDA.SET_OF_BOOKS_ID PDA_SET_OF_BOOKS_ID,
            PDA.CODE_COMBINATION_ID PDA_CODE_COMBINATION_ID,
            gcck.SEGMENT1 PO_ACCOUNT_BU_GL_SGT1,
            gcck.SEGMENT2 PO_ACCOUNT_LOCATION_GL_SGT2,
            gcck.SEGMENT3 PO_ACCOUNT_DEPARTMENT_GL_SGT3,
            gcck.SEGMENT4 PO_ACCOUNT_NATURALACCOUNT_GL_SGT4,
            gcck.SEGMENT5 PO_ACCOUNT_PRODUCTGROUP_GL_SGT5,
            gcck.SEGMENT6 PO_ACCOUNT_INTERCOMPANY_GL_SGT6,
            PDA.QUANTITY_ORDERED PDA_QUANTITY_ORDERED,
            PDA.CREATION_DATE PDA_CREATION_DATE,
            fupda.user_name PDA_CREATED_BY,
            PDA.QUANTITY_DELIVERED PDA_QUANTITY_DELIVERED,
            PDA.QUANTITY_BILLED PDA_QUANTITY_BILLED,
            PDA.QUANTITY_CANCELLED PDA_QUANTITY_CANCELLED,
            PDA.REQ_HEADER_REFERENCE_NUM PDA_REQ_HEADER_REFERENCE_NUM,
            PDA.REQ_LINE_REFERENCE_NUM PDA_REQ_LINE_REFERENCE_NUM,
            PDA.REQ_DISTRIBUTION_ID PDA_REQ_DISTRIBUTION_ID,
            PDA.DELIVER_TO_LOCATION_ID PDA_DELIVER_TO_LOCATION_ID,
            PDA.DELIVER_TO_PERSON_ID PDA_DELIVER_TO_PERSON_ID,
            PDA.RATE_DATE PDA_RATE_DATE,
            PDA.RATE PDA_RATE,
            PDA.AMOUNT_BILLED PDA_AMOUNT_BILLED,
            PDA.ACCRUED_FLAG PDA_ACCRUED_FLAG,
            PDA.GL_CANCELLED_DATE PDA_GL_CANCELLED_DATE,
            PDA.DESTINATION_TYPE_CODE PDA_DESTINATION_TYPE_CODE,
            PDA.DESTINATION_ORGANIZATION_ID PDA_DESTINATION_ORGANIZATION_ID,
            PDA.DESTINATION_SUBINVENTORY PDA_DESTINATION_SUBINVENTORY,
            PDA.ATTRIBUTE_CATEGORY PDA_ATTRIBUTE_CATEGORY,
            CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE1 ELSE NULL END AS PDA_DFF_COUNTRY,
            CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE2 ELSE NULL END AS PDA_DFF_STATE,
            CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE3 ELSE NULL END AS PDA_DFF_COUNTY,
            CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE4 ELSE NULL END AS PDA_DFF_CITY,
            CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE5 ELSE NULL END AS PDA_DFF_SITE,
            CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE6 ELSE NULL END AS PDA_DFF_DEPT,
            CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE9 ELSE NULL END AS PDA_DFF_RSN_ASSET_CREATION,
            CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE10 ELSE NULL END AS PDA_DFF_GPAO_PROD_ORDER_NUMBER,
            CASE WHEN PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND PDA.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN PDA.ATTRIBUTE11 ELSE NULL END AS PDA_DFF_PRODUCT_GROUP,
            PDA.ATTRIBUTE15       PDA_DFF_AR_NUMBER,
            PDA.WIP_ENTITY_ID PDA_WIP_ENTITY_ID,
            PDA.WIP_OPERATION_SEQ_NUM PDA_WIP_OPERATION_SEQ_NUM,
            PDA.WIP_RESOURCE_SEQ_NUM PDA_WIP_RESOURCE_SEQ_NUM,
            PDA.BOM_RESOURCE_ID PDA_BOM_RESOURCE_ID,
            PDA.ACCRUAL_ACCOUNT_ID PDA_ACCRUAL_ACCOUNT_ID,
            PDA.VARIANCE_ACCOUNT_ID PDA_VARIANCE_ACCOUNT_ID,
            PDA.PREVENT_ENCUMBRANCE_FLAG PDA_PREVENT_ENCUMBRANCE_FLAG,
            PDA.DESTINATION_CONTEXT PDA_DESTINATION_CONTEXT,
            PDA.DISTRIBUTION_NUM PDA_DISTRIBUTION_NUM,
            PDA.ACCRUE_ON_RECEIPT_FLAG PDA_ACCRUE_ON_RECEIPT_FLAG,
            PDA.TAX_RECOVERY_OVERRIDE_FLAG PDA_TAX_RECOVERY_OVERRIDE_FLAG,
            PDA.RECOVERABLE_TAX PDA_RECOVERABLE_TAX,
            PDA.NONRECOVERABLE_TAX PDA_NONRECOVERABLE_TAX,
            PDA.DISTRIBUTION_TYPE PDA_DISTRIBUTION_TYPE,
            PDA.AMOUNT_FUNDED PDA_AMOUNT_FUNDED,
            PDA.FUNDED_VALUE PDA_FUNDED_VALUE,
            PDA.PARTIAL_FUNDED_FLAG PDA_PARTIAL_FUNDED_FLAG,
            PDA.QUANTITY_FUNDED PDA_QUANTITY_FUNDED,
            PDA.CHANGE_IN_FUNDED_VALUE PDA_CHANGE_IN_FUNDED_VALUE,
            ROWNUM ROW_NUMBER_ID,
            SYSDATE ROW_CREATION_DATE,
            SYSDATE ROW_LAST_UPDATE_DATE,
            sgt5.PRODUCT_GROUP_GL_DESCRIPTION,
            hla.LOCATION_CODE SHIP_TO_LOCATION_CODE,
            decode(sgt5.PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT, null, sgt2.LOCATION_GL_BU_VENTE, fvb.VALEUR)   as BU_NEW,
            odt.GSOB_CURRENCY_CODE as OU_CURRENCY,
            qr.character6 LEAD_TIME_TO_NAMED_PLACE_LS  --> qr.character6 : mis en commentaire car cela remontre trop de doublon , cas � �tudier
        FROM steph_apps_po_distributions_all_bz pda
        LEFT JOIN steph_apps_po_line_locations_all_bz pll ON pda.line_location_id = pll.line_location_id
        LEFT JOIN steph_apps_po_lines_all_bz pl ON pll.po_line_id  = pl.po_line_id
        LEFT JOIN steph_apps_po_headers_all_bz ph on pl.po_header_id = ph.po_header_id 
        LEFT JOIN OU_DETAILS_TEMP odt ON PH.ORG_ID = odt.HOU_ORGANIZATION_ID
        LEFT JOIN IO_DETAILS_TEMP idt ON pll.SHIP_TO_ORGANIZATION_ID = idt.OOD_ORGANIZATION_ID
        LEFT JOIN STEPH_APPS_AP_TERMS_BZ aat ON PH.TERMS_ID = aat.term_id
        LEFT JOIN STEPH_APPS_FND_USER_bz fuph ON ph.CREATED_BY = fuph.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fuphb ON ph.LAST_UPDATED_BY = fuphb.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fupl ON pl.CREATED_BY = fupl.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fuplb ON pl.LAST_UPDATED_BY = fuplb.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fuplc ON pl.CANCELLED_BY = fuplc.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fuplcl ON pl.CLOSED_BY = fuplcl.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fupll ON pll.CREATED_BY = fupll.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fupllb ON pll.LAST_UPDATED_BY = fupllb.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fupllc ON pll.CANCELLED_BY = fupllc.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fupllcl ON pll.CLOSED_BY = fupllcl.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fupda ON pda.CREATED_BY = fupda.user_ID
        LEFT JOIN STEPH_APPS_FND_USER_bz fupdab ON pda.LAST_UPDATED_BY = fupdab.user_ID
        LEFT JOIN STEPH_APPS_GL_CODE_COMBINATIONS_KFV_BZ gcck on PDA.CODE_COMBINATION_ID = gcck.CODE_COMBINATION_ID
        LEFT JOIN STEPH_APPS_HR_LOCATIONS_ALL_BZ HLA on pll.ship_to_location_id = hla.location_id
        LEFT JOIN FILE_PRODUCTGROUP_SEGMENT5_BZ sgt5 on nvl(pda.aTtribute11,gcck.SEGMENT5) = sgt5.product_group_gl_code
        /*  LEFT JOIN FILE_VAR_BU_BZ fvb on sgt5.PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT = fvb.subbu
          remplacer par la jointure avec FILE_TRANSFORMATION_DATA*/
            LEFT JOIN FILE_TRANSFORMATION_DATA_BZ fvb
                     ON fvb.cle = sgt5.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE
                     AND fvb.type = 'BU_NEW'
        LEFT JOIN FILE_LOCATION_SEGMENT2_BZ sgt2 on gcck.SEGMENT2 = sgt2.location_gl_code
   /* mis en commentaire car cela remontre trop de doublon , cas � �tudier */
   /*     LEFT JOIN steph_apps_qa_results_bz qr ON
                qr.item_id = to_char(PL.ITEM_ID)
                AND qr.vendor_id = PH.VENDOR_ID
                AND qr.character5 = PH.FOB_LOOKUP_CODE
                AND qr.character1 = SUBSTR(ph.FREIGHT_TERMS_LOOKUP_CODE, -3)
                and qr.plan_id = ( select qp.plan_id from steph_apps_qa_plans_bz qp where qp.name = 'LEAD TIME TO NAMED PLACE LS'  )
     */
       LEFT JOIN (
              SELECT *
              FROM (
                    SELECT /*+ NO_MERGE MATERIALIZE */
                        qr.*, 
                       ROW_NUMBER() OVER (PARTITION BY item_id, vendor_id, character5, character1, plan_id ORDER BY qr.last_update_date DESC) rn
                       FROM steph_apps_qa_results_bz qr
                       WHERE qr.plan_id = (
                  SELECT qp.plan_id 
                  FROM steph_apps_qa_plans_bz qp 
                  WHERE qp.name LIKE '%LEAD TIME TO NAMED PLACE LS%'
                )
              )
              WHERE rn = 1
            ) qr ON 
              qr.item_id = TO_CHAR(PL.ITEM_ID)
              AND qr.vendor_id = PH.VENDOR_ID
              AND qr.character5 = PH.FOB_LOOKUP_CODE
              AND qr.character1 = SUBSTR(PH.FREIGHT_TERMS_LOOKUP_CODE, -3)

          WHERE  1 = 1
             and PH.CREATION_DATE >= '01/01/2015'  ---> date d'entr�e de LS dans la module PO oracle
             and ph.type_lookup_code = 'STANDARD'         
                
                ;

     g_status   := 'COMPLETED';
     g_etape    := '010 - INSERT INTO' ;
     Write_Log_PROC;

/* en commentaire de POTC le 17/03/25 pour privil�gier un appel une fois par semaine dans LH2_DTH_SILVER_GATHER_STAT_PKG
     g_table     := 'PO_STANDARD'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('PO_STANDARD'); 
     g_status   := 'COMPLETED';
     g_etape    := '099 - STATS' ; 
     Write_Log_PROC;
*/

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
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

   END PO_STANDARD_PROC;
   
   
    /****************************************************************************************
    * PROCEDURE   :  REQUISITION_INTERNAL_PROC
    * DESCRIPTION :  Create REQUISITION_INTERNAL TABLE 
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
   PROCEDURE REQUISITION_INTERNAL_PROC
   IS   v_procedure varchar2(100) := 'REQUISITION_INTERNAL_PROC';
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

     g_table     := 'REQUISITION_INTERNAL'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE REQUISITION_INTERNAL';
     g_status   := 'COMPLETED';
     g_etape    := '011 - TRUNCATE TABLE' ;
     Write_Log_PROC;

     g_table     := 'REQUISITION_INTERNAL'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'   ;
     
        INSERT /*+APPEND PARALLEL(REQUISITION_INTERNAL, 8) MONITOR gather_plan_statistics*/ INTO REQUISITION_INTERNAL
        select	/*+
                      PARALLEL(rd 6)
                      PARALLEL(fuph 6)
                      PARALLEL(fuphb 6)
                      PARALLEL(rl 6)
                      PARALLEL(fuph2 6)
                      PARALLEL(fuphb2 6)
                      PARALLEL(rh 6)
                      PARALLEL(odt 6)
                      PARALLEL(fuph3 6)
                      PARALLEL(fuphb3 6)
                      PARALLEL(idt 6)
                      PARALLEL(gcck 6)
                      PARALLEL(HLA 6)
                      PARALLEL(sgt5 6)
                      PARALLEL(fvb 6)
                      PARALLEL(sgt2 6)
        gather_plan_statistics
    */
                RD.DISTRIBUTION_ID	RD_DISTRIBUTION_ID,
                RD.LAST_UPDATE_DATE	RD_LAST_UPDATE_DATE,
                fuphb.user_name	RD_LAST_UPDATED_BY,
                RD.SET_OF_BOOKS_ID	RD_SET_OF_BOOKS_ID,
                RD.CODE_COMBINATION_ID	RD_CODE_COMBINATION_ID,
                gcck.SEGMENT1	REQ_ACCOUNT_BU_GL_SGT1,
                gcck.SEGMENT2	REQ_ACCOUNT_LOCATION_GL_SGT2,
                gcck.SEGMENT3	REQ_ACCOUNT_DEPARTMENT_GL_SGT3,
                gcck.SEGMENT4	REQ_ACCOUNT_NATURALACCOUNT_GL_SGT4,
                gcck.SEGMENT5	REQ_ACCOUNT_PRODUCTGROUP_GL_SGT5,
                gcck.SEGMENT6	REQ_ACCOUNT_INTERCOMPANY_GL_SGT6,
                RD.REQ_LINE_QUANTITY	RD_REQ_LINE_QUANTITY,
                RD.CREATION_DATE	RD_CREATION_DATE,
                fuph.user_name	RD_CREATED_BY,
                RD.BUDGET_ACCOUNT_ID	RD_BUDGET_ACCOUNT_ID,
                RD.ACCRUAL_ACCOUNT_ID	RD_ACCRUAL_ACCOUNT_ID,
                RD.VARIANCE_ACCOUNT_ID	RD_VARIANCE_ACCOUNT_ID,
                RD.PREVENT_ENCUMBRANCE_FLAG	RD_PREVENT_ENCUMBRANCE_FLAG,
                RD.ATTRIBUTE_CATEGORY	RD_ATTRIBUTE_CATEGORY,
                CASE WHEN RD.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RD.ATTRIBUTE1 ELSE NULL end as RD_DFF_COUNTRY,
                CASE WHEN RD.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RD.ATTRIBUTE2 ELSE NULL end as RD_DFF_STATE,
                CASE WHEN RD.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RD.ATTRIBUTE3 ELSE NULL end as RD_DFF_COUNTY,
                CASE WHEN RD.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RD.ATTRIBUTE4 ELSE NULL end as RD_DFF_CITY,
                CASE WHEN RD.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RD.ATTRIBUTE5 ELSE NULL end as RD_DFF_SITE,
                CASE WHEN RD.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RD.ATTRIBUTE6 ELSE NULL end as RD_DFF_DEPT,
                CASE WHEN RD.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RD.ATTRIBUTE1 ELSE NULL end as RD_DFF_RSN_ASSET_CREATION,
                CASE WHEN RD.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RD.ATTRIBUTE10 ELSE NULL end as RD_DFF_LEGACY_MFG_NUMBER,
                CASE WHEN RD.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RD.ATTRIBUTE11 ELSE NULL end as RD_DFF_PRODUCT_GROUP,
                CASE WHEN RD.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RD.ATTRIBUTE15 ELSE NULL end as RD_DFF_AR_NUMBER,
                RD.DISTRIBUTION_NUM	RD_DISTRIBUTION_NUM,
                RD.ORG_ID	RD_ORG_ID,
                odt.Hou_name OU_NAME,
                RD.RECOVERABLE_TAX	RD_RECOVERABLE_TAX,
                RD.NONRECOVERABLE_TAX	RD_NONRECOVERABLE_TAX,
                RD.TAX_RECOVERY_OVERRIDE_FLAG	RD_TAX_RECOVERY_OVERRIDE_FLAG,
                RD.AMOUNT_FUNDED	RD_AMOUNT_FUNDED,
                RD.FUNDED_VALUE	RD_FUNDED_VALUE,
                RD.PARTIAL_FUNDED_FLAG	RD_PARTIAL_FUNDED_FLAG,
                RD.QUANTITY_FUNDED	RD_QUANTITY_FUNDED,
                RL.REQUISITION_LINE_ID	RL_REQUISITION_LINE_ID,
                RL.LINE_NUM	RL_LINE_NUM,
                RL.LINE_TYPE_ID	RL_LINE_TYPE_ID,
                RL.CATEGORY_ID	RL_CATEGORY_ID,
                RL.ITEM_DESCRIPTION	RL_ITEM_DESCRIPTION,
                RL.UNIT_MEAS_LOOKUP_CODE	RL_UNIT_MEAS_LOOKUP_CODE,
                RL.UNIT_PRICE	RL_UNIT_PRICE,
                RL.QUANTITY	RL_QUANTITY,
                RL.DELIVER_TO_LOCATION_ID	RL_DELIVER_TO_LOCATION_ID,
                LH2_DTH_SILVER_FUNCTIONS_PKG.GET_STEPH_APPS_PER_ALL_PEOPLE_F_NAME_FUNC(RL.TO_PERSON_ID)	RL_TO_PERSON_NAME,
                RL.LAST_UPDATE_DATE	RL_LAST_UPDATE_DATE,
                fuphb2.user_name	RL_LAST_UPDATED_BY,
                RL.SOURCE_TYPE_CODE	RL_SOURCE_TYPE_CODE,
                RL.CREATION_DATE	RL_CREATION_DATE,
                fuph2.user_name	RL_CREATED_BY,
                RL.ITEM_ID	RL_ITEM_ID,
                RL.ITEM_REVISION	RL_ITEM_REVISION,
                RL.QUANTITY_DELIVERED	RL_QUANTITY_DELIVERED,
                LH2_DTH_SILVER_FUNCTIONS_PKG.GET_STEPH_APPS_PER_ALL_PEOPLE_F_NAME_FUNC(RL.SUGGESTED_BUYER_ID)	RL_SUGGESTED_BUYER_NAME,
                RL.ENCUMBERED_FLAG	RL_ENCUMBERED_FLAG,
                RL.RFQ_REQUIRED_FLAG	RL_RFQ_REQUIRED_FLAG,
                RL.NEED_BY_DATE	RL_NEED_BY_DATE,
                RL.LINE_LOCATION_ID	RL_LINE_LOCATION_ID,
                RL.JUSTIFICATION	RL_JUSTIFICATION,
                RL.NOTE_TO_AGENT	RL_NOTE_TO_AGENT,
                RL.NOTE_TO_RECEIVER	RL_NOTE_TO_RECEIVER,
                RL.DOCUMENT_TYPE_CODE	RL_DOCUMENT_TYPE_CODE,
                RL.BLANKET_PO_HEADER_ID	RL_BLANKET_PO_HEADER_ID,
                RL.BLANKET_PO_LINE_NUM	RL_BLANKET_PO_LINE_NUM,
                RL.CURRENCY_CODE	RL_CURRENCY_CODE,
                RL.RATE_TYPE	RL_RATE_TYPE,
                RL.RATE_DATE	RL_RATE_DATE,
                RL.RATE	RL_RATE,
                RL.CURRENCY_UNIT_PRICE	RL_CURRENCY_UNIT_PRICE,
                RL.SUGGESTED_VENDOR_NAME	RL_SUGGESTED_VENDOR_NAME,
                RL.SUGGESTED_VENDOR_LOCATION	RL_SUGGESTED_VENDOR_LOCATION,
                RL.SUGGESTED_VENDOR_PRODUCT_CODE	RL_SUGGESTED_VENDOR_PRODUCT_CODE,
                RL.HAZARD_CLASS_ID	RL_HAZARD_CLASS_ID,
                RL.REFERENCE_NUM	RL_REFERENCE_NUM,
                RL.ON_RFQ_FLAG	RL_ON_RFQ_FLAG,
                RL.URGENT_FLAG	RL_URGENT_FLAG,
                RL.CANCEL_FLAG	RL_CANCEL_FLAG,
                RL.SOURCE_ORGANIZATION_ID	RL_SOURCE_ORGANIZATION_ID,
                RL.SOURCE_SUBINVENTORY	RL_SOURCE_SUBINVENTORY,
                RL.DESTINATION_TYPE_CODE	RL_DESTINATION_TYPE_CODE,
                RL.DESTINATION_ORGANIZATION_ID	RL_DESTINATION_ORGANIZATION_ID,
                RL.DESTINATION_SUBINVENTORY	RL_DESTINATION_SUBINVENTORY,
                RL.QUANTITY_CANCELLED	RL_QUANTITY_CANCELLED,
                RL.CANCEL_DATE	RL_CANCEL_DATE,
                RL.CANCEL_REASON	RL_CANCEL_REASON,
                RL.CLOSED_CODE	RL_CLOSED_CODE,
                RL.CHANGED_AFTER_RESEARCH_FLAG	RL_CHANGED_AFTER_RESEARCH_FLAG,
                RL.VENDOR_ID	RL_VENDOR_ID,
                RL.VENDOR_SITE_ID	RL_VENDOR_SITE_ID,
                RL.VENDOR_CONTACT_ID	RL_VENDOR_CONTACT_ID,
                RL.WIP_ENTITY_ID	RL_WIP_ENTITY_ID,
                RL.WIP_OPERATION_SEQ_NUM	RL_WIP_OPERATION_SEQ_NUM,
                RL.WIP_RESOURCE_SEQ_NUM	RL_WIP_RESOURCE_SEQ_NUM,
                RL.ATTRIBUTE_CATEGORY	RL_ATTRIBUTE_CATEGORY,
                RL.DESTINATION_CONTEXT	RL_DESTINATION_CONTEXT,
                CASE WHEN RL.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RL.ATTRIBUTE4 ELSE NULL end as RL_DFF_SALES_ORDER_NUMBER,
                CASE WHEN RL.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RL.ATTRIBUTE6 ELSE NULL end as RL_DFF_MODE_OF_DISTRIBUTION,
                CASE WHEN RL.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RL.ATTRIBUTE7 ELSE NULL end as RL_DFF_SALES_ORDER_LINE_NUMBER,
                CASE WHEN RL.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RL.ATTRIBUTE8 ELSE NULL end as RL_DFF_PRICE,
                CASE WHEN RL.ATTRIBUTE_CATEGORY LIKE 'LS %' THEN RL.ATTRIBUTE9 ELSE NULL end as RL_DFF_CURRENCY,
                RL.BOM_RESOURCE_ID	RL_BOM_RESOURCE_ID,
                RL.CLOSED_REASON	RL_CLOSED_REASON,
                RL.CLOSED_DATE	RL_CLOSED_DATE,
                RL.QUANTITY_RECEIVED	RL_QUANTITY_RECEIVED,
                RL.MANUFACTURER_ID	RL_MANUFACTURER_ID,
                RL.MANUFACTURER_NAME	RL_MANUFACTURER_NAME,
                RL.MANUFACTURER_PART_NUMBER	RL_MANUFACTURER_PART_NUMBER,
                RL.VMI_FLAG	RL_VMI_FLAG,
                RL.REQS_IN_POOL_FLAG	RL_REQS_IN_POOL_FLAG,
                RL.DROP_SHIP_FLAG	RL_DROP_SHIP_FLAG,
                RL.NEGOTIATED_BY_PREPARER_FLAG	RL_NEGOTIATED_BY_PREPARER_FLAG,
                RL.SHIP_METHOD	RL_SHIP_METHOD,
                RL.BASE_UNIT_PRICE	RL_BASE_UNIT_PRICE,
                RL.TRANSFERRED_TO_OE_FLAG	RL_TRANSFERRED_TO_OE_FLAG,
                RH.REQUISITION_HEADER_ID	RH_REQUISITION_HEADER_ID,
                LH2_DTH_SILVER_FUNCTIONS_PKG.GET_STEPH_APPS_PER_ALL_PEOPLE_F_NAME_FUNC(RH.PREPARER_ID)	RH_PREPARER_NAME,
                RH.LAST_UPDATE_DATE	RH_LAST_UPDATE_DATE,
                fuphb3.user_name	RH_LAST_UPDATED_BY,
                RH.SEGMENT1	REQUISITION_NUMBER,
                RH.SUMMARY_FLAG	RH_SUMMARY_FLAG,
                RH.ENABLED_FLAG	RH_ENABLED_FLAG,
                RH.CREATION_DATE	RH_CREATION_DATE,
                fuph3.user_name	RH_CREATED_BY,
                RH.DESCRIPTION	RH_DESCRIPTION,
                RH.AUTHORIZATION_STATUS	RH_AUTHORIZATION_STATUS,
                RH.TYPE_LOOKUP_CODE	RH_TYPE_LOOKUP_CODE,
                RH.TRANSFERRED_TO_OE_FLAG	RH_TRANSFERRED_TO_OE_FLAG,
                RH.PRELIMINARY_RESEARCH_FLAG	RH_PRELIMINARY_RESEARCH_FLAG,
                RH.INTERFACE_SOURCE_CODE	RH_INTERFACE_SOURCE_CODE,
                RH.INTERFACE_SOURCE_LINE_ID	RH_INTERFACE_SOURCE_LINE_ID,
                RH.CLOSED_CODE	RH_CLOSED_CODE,
                RH.WF_ITEM_TYPE	RH_WF_ITEM_TYPE,
                RH.WF_ITEM_KEY	RH_WF_ITEM_KEY,
                RH.APPROVED_DATE	RH_APPROVED_DATE,
                ROWNUM	ROW_NUMBER_ID,
                SYSDATE	ROW_CREATION_DATE,
                SYSDATE	ROW_LAST_UPDATE_DATE,
                sgt5.PRODUCT_GROUP_GL_DESCRIPTION,
                hla.LOCATION_CODE SHIP_TO_LOCATION_CODE,
                decode(sgt5.PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT, null, sgt2.LOCATION_GL_BU_VENTE, fvb.VALEUR)   as BU_NEW,
                idt.OOD_ORGANIZATION_CODE IO_CODE,
                odt.GSOB_CURRENCY_CODE as OU_CURRENCY,
                To_NUMBER(NULL) LEAD_TIME_TO_NAMED_PLACE_LS
            FROM steph_apps_PO_req_distributions_all_bz rd 
            LEFT JOIN STEPH_APPS_FND_USER_bz fuph ON rd.CREATED_BY = fuph.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuphb ON rd.LAST_UPDATED_BY = fuphb.user_ID
            LEFT JOIN STEPH_APPS_GL_CODE_COMBINATIONS_KFV_BZ gcck on rd.CODE_COMBINATION_ID = gcck.CODE_COMBINATION_ID
            LEFT JOIN OU_DETAILS_TEMP odt ON rd.ORG_ID = odt.HOU_ORGANIZATION_ID
            LEFT JOIN steph_apps_po_requisition_lines_all_bz rl on  rl.requisition_line_id = rd.requisition_line_id 
            LEFT JOIN STEPH_APPS_FND_USER_bz fuph2 ON rl.CREATED_BY = fuph2.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuphb2 ON rl.LAST_UPDATED_BY = fuphb2.user_ID
            LEFT JOIN steph_apps_po_requisition_headers_all_bz rh ON rh.requisition_header_id = rl.requisition_header_id
            LEFT JOIN STEPH_APPS_FND_USER_bz fuph3 ON rh.CREATED_BY = fuph3.user_ID
            LEFT JOIN STEPH_APPS_FND_USER_bz fuphb3 ON rh.LAST_UPDATED_BY = fuphb3.user_ID
            LEFT JOIN STEPH_APPS_HR_LOCATIONS_ALL_BZ HLA on rl.DELIVER_TO_LOCATION_ID = hla.location_id
            LEFT JOIN FILE_PRODUCTGROUP_SEGMENT5_BZ sgt5 on nvl(rd.attribute11,gcck.segment5) = sgt5.product_group_gl_code
            /*  LEFT JOIN FILE_VAR_BU_BZ fvb on sgt5.PRODUCT_GROUP_GL_REGROUPEMENT_ACHAT = fvb.subbu
          remplacer par la jointure avec FILE_TRANSFORMATION_DATA*/
            LEFT JOIN FILE_TRANSFORMATION_DATA_BZ fvb
                     ON fvb.cle = sgt5.PRODUCT_GROUP_GL_REGROUPEMENT_VENTE
                     AND fvb.type = 'BU_NEW'
            LEFT JOIN IO_DETAILS_TEMP idt ON rl.DESTINATION_ORGANIZATION_ID = idt.OOD_ORGANIZATION_ID
            LEFT JOIN FILE_LOCATION_SEGMENT2_BZ sgt2 on gcck.SEGMENT2 = sgt2.location_gl_code
            WHERE  1=1	
                  and rh.TYPE_LOOKUP_CODE = 'INTERNAL'
             ;
     g_status   := 'COMPLETED';
     g_etape    := '010 - INSERT INTO' ;
     Write_Log_PROC;


     g_table     := 'REQUISITION_INTERNAL'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('REQUISITION_INTERNAL'); 
     g_status   := 'COMPLETED';
     g_etape    := '099 - STATS' ; 
     Write_Log_PROC;

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
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

   END REQUISITION_INTERNAL_PROC;
   
   
   /****************************************************************************************
    * PROCEDURE   :  PO_CONTRACT
    * DESCRIPTION :  Create PO_CONTRACT TABLE 
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/
   PROCEDURE PO_CONTRACT_PROC
   IS   v_procedure varchar2(100) := 'PO_CONTRACT_PROC';
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

     g_table     := 'PO_CONTRACT'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE PO_CONTRACT';
        
     g_status   := 'COMPLETED';
     g_etape    := '011 - TRUNCATE TABLE' ;
     Write_Log_PROC;

     g_table     := 'PO_CONTRACT'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'   ;
     
        INSERT INTO   PO_CONTRACT
        select
    pha.BZ_BIS_CREATION_DATE,
    pha.BZ_BIS_UPDATED_DATE,
    pha.FETCH_YEAR,
    pha.FETCH_MONTH,
    pha.FETCH_DAY,
    pha.PO_HEADER_ID,
    To_CHAR(pha.PO_HEADER_ID) PO_HEADER_ID_CHAR,
    odt.HOU_NAME OU_NAME,
    pha.AGENT_ID,
    LH2_DTH_SILVER_FUNCTIONS_PKG.GET_STEPH_APPS_PER_ALL_PEOPLE_F_NAME_FUNC(pha.AGENT_ID) PO_BUYER,
    pha.TYPE_LOOKUP_CODE,
    pha.LAST_UPDATE_DATE,
    saa1.user_name LAST_UPDATED_BY,
    pha.SEGMENT1 CONTRACT_NUMBER,
    pha.SUMMARY_FLAG,
    pha.ENABLED_FLAG,
    pha.LAST_UPDATE_LOGIN,
    pha.CREATION_DATE,
    saa2.user_name CREATED_BY,
    pha.VENDOR_ID,
    pha.VENDOR_SITE_ID,
    pha.VENDOR_CONTACT_ID,
    pha.SHIP_TO_LOCATION_ID,
    pha.BILL_TO_LOCATION_ID,
    pha.TERMS_ID,
    pha.FOB_LOOKUP_CODE,
    pha.FREIGHT_TERMS_LOOKUP_CODE,
    pha.CURRENCY_CODE,
    pha.RATE_TYPE,
    pha.RATE_DATE,
    pha.RATE,
    pha.FROM_HEADER_ID,
    pha.FROM_TYPE_LOOKUP_CODE,
    pha.BLANKET_TOTAL_AMOUNT,
    pha.AUTHORIZATION_STATUS,
    pha.REVISION_NUM,
    pha.REVISED_DATE,
    pha.APPROVED_FLAG,
    pha.APPROVED_DATE,
    pha.AMOUNT_LIMIT,
    pha.PRINT_COUNT,
    pha.PRINTED_DATE,
    pha.CONFIRMING_ORDER_FLAG,
    pha.COMMENTS,
    pha.ACCEPTANCE_REQUIRED_FLAG,
    pha.CLOSED_DATE,
    pha.CANCEL_FLAG,
    pha.FIRM_STATUS_LOOKUP_CODE,
    pha.FROZEN_FLAG,
    pha.SUPPLY_AGREEMENT_FLAG,
    pha.ATTRIBUTE_CATEGORY,
    CASE WHEN pha.ATTRIBUTE_CATEGORY NOT LIKE 'EPT %' AND pha.ATTRIBUTE_CATEGORY NOT LIKE 'EGS %' THEN pha.ATTRIBUTE5 ELSE NULL END AS DFF_MANUFACTURER,
    pha.CLOSED_CODE,
    pha.REQUEST_ID,
    pha.ORG_ID,
    pha.WF_ITEM_TYPE,
    pha.WF_ITEM_KEY,
    pha.GLOBAL_AGREEMENT_FLAG,
    pha.PENDING_SIGNATURE_FLAG,
    pha.DOCUMENT_CREATION_METHOD,
    pha.SUBMIT_DATE,
    pha.SUPPLIER_NOTIF_METHOD,
    pha.CREATED_LANGUAGE,
    pha.STYLE_ID,
    pha.ENABLE_ALL_SITES,
    pha.CLM_DOCUMENT_NUMBER,
    pha.CLM_EFFECTIVE_DATE,
    pha.FETCH_DATE,
    ROWNUM AS ROW_NUMBER_ID,  
    SYSDATE AS ROW_CREATION_DATE,  
    SYSDATE AS ROW_LAST_UPDATE_DATE
FROM  steph_apps_po_headers_all_bz pha

LEFT JOIN STEPH_APPS_FND_USER_bz saa2 ON
  --  pha.BZ_BIS_CREATED_BY = saa2.user_ID -- modif le 13/10/25 POTC
    pha.CREATED_BY = saa2.user_ID 

LEFT JOIN STEPH_APPS_FND_USER_bz saa1 ON
  --  pha.BZ_BIS_UPTATED_BY = saa1.user_ID   -- modif le 13/10/25 POTC
    pha.LAST_UPDATED_BY = saa1.user_ID 

LEFT JOIN OU_DETAILS_TEMP odt ON 
    pha.ORG_ID = odt.HOU_ORGANIZATION_ID

    WHERE  1=1 
    and pha.type_lookup_code = 'CONTRACT'
  ;
  
  g_status   := 'COMPLETED';
     g_etape    := '010 - INSERT INTO' ;
     Write_Log_PROC;


     g_table     := 'PO_CONTRACT'; 
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        LH2_SILVER_ADMIN_PKG.GATHER_TABLE_STATS_PROC('PO_CONTRACT'); 
     g_status   := 'COMPLETED';
     g_etape    := '099 - STATS' ; 
     Write_Log_PROC;

     g_table     := v_procedure;
     g_date_deb  := sysdate;
     g_status    := 'WIP';
     g_etape     := $$plsql_line + 1  || ' - num error line'  ; 
        COMMIT;
     g_status   := 'COMPLETED';
     g_etape    := '100 - COMMIT';	
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

   END PO_CONTRACT_PROC;
    /****************************************************************************************
    * PROCEDURE   :  MAIN
    * DESCRIPTION :  Procedure principale
    *                
    * PARAMETRES  :
    * NOM               TYPE        DESCRIPTION
    * -------------------------------------------------------------------------------------
    * <parameter>      <TYPE>      <Desc>
    ****************************************************************************************/

	PROCEDURE MAIN (pv_errbuf   OUT VARCHAR2
                  ,pn_retcode  OUT NUMBER
				  )
   IS
	 v_procedure            varchar2(100)   := 'MAIN';
	 v_status              varchar2(1) := 'A';  --statut Accept� (sinon R pour Rejet�)
	 v_message             varchar2(1000) := NULL;
     v_date_deb_pkg     TIMESTAMP := sysdate;

   BEGIN  --D�but traitement
	 DBMS_OUTPUT.ENABLE (1000000);

     g_level := 'S';
	 g_programme := $$plsql_unit || '.' || v_procedure ;
     g_table     := $$plsql_unit;
     g_date_deb  := v_date_deb_pkg;
     g_status   := 'BEGIN';
     g_etape    := '0001 - Begin PKG';
     Write_Log_PROC;

	 DBMS_OUTPUT.PUT_LINE (g_programme);
	 DBMS_OUTPUT.PUT_LINE ('-----------------------------------------------------------------------');
	 DBMS_OUTPUT.PUT_LINE ('------------------------START--------------------------------');

     v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
	 DBMS_OUTPUT.PUT_LINE (v_message);
	 DBMS_OUTPUT.PUT_LINE ('Lancement de LH2_DTH_PURCHASE_ORDER_PKG.MAIN');

     v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
	 DBMS_OUTPUT.PUT_LINE (v_message);
	 DBMS_OUTPUT.PUT_LINE ('Lancement de PO_BLANKET_PROC');
     PO_BLANKET_PROC;

     v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
	 DBMS_OUTPUT.PUT_LINE (v_message);
	 DBMS_OUTPUT.PUT_LINE ('Lancement de PO_STANDARD_PROC');
     PO_STANDARD_PROC;

     v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
	 DBMS_OUTPUT.PUT_LINE (v_message);
	 DBMS_OUTPUT.PUT_LINE ('Lancement de REQUISITION_INTERNAL_PROC');
     REQUISITION_INTERNAL_PROC;

     v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
	 DBMS_OUTPUT.PUT_LINE (v_message);
	 DBMS_OUTPUT.PUT_LINE ('Lancement de PO_CONTRACT_PROC');
     PO_CONTRACT_PROC;
     
     v_message:= to_char(sysdate,'DD-MM-RRRR HH24:MI:SS');
	 DBMS_OUTPUT.PUT_LINE (v_message);
	 DBMS_OUTPUT.PUT_LINE ('-----------------------------END--------------------------------------');--fin compte-rendu

    if g_erreur_pkg = 1 then 
         pn_retcode := 1;
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
      WHEN OTHERS
      THEN
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

END LH2_DTH_SILVER_PURCHASE_ORDER_PKG;