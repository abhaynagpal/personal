INSERT INTO
  `mpc-dev-459470.DELEVERIES.C_TMP_EM_PIVOT`
(
EHGUID,
CALQUART1,
ARTICLEID,
ARTCRTDT,
ARTCRTTM,
CONSIGNID,
MANIFSTID,
MERLOCID,
PRODUCT,
SUBPROD,
SERVICE,
CUSTREC,
BPARTNER,
LOD_DATE,
DLVRNTWRK,
PCODEFROM,
SNDRPCODE,
TIMETABLE,
WCLODGE,
ACC1STDT,
DEL1STDT,
DELDATE,
DRV1STACC,
DRV1STDEL,
PCODEREC,
DEDD,
STOP_CK,
STOP_DATE,
STOP_TIME,
WC1STACC,
WC1STDEL,
PCODETO,
EDD,
FPFWKCTR,
FPFEVENT,
FPFSRC,
FPFDATEL,
FPFTIMEL,
FPFDVSRC,
FPFMSDTL,
FPFMSTML,
FPFWCCTYP,
FPFMISEVNT,
FPFSTATUS,
FPEVENT,
FPWKCTR,
FPSRC,
FPDATEL,
FPTIMEL,
FPDVSRC,
FPMSDTL,
FPMSTML,
FPWCCTYP,
FFMISEVNT,
FPOKCNT,
OBWKCTR,
OBEVENT,
OBSRC,
OBDATEL,
OBTIMEL,
OBUSER,
OBUROLE,
OBCNTRCT,
OBSTATUS,
OBMSDTL,
OBMSTML,
FDLWKCTR,
FDLDATEL,
FDLTIMEL,
FDLEVENT,
FDLSRC,
FDLUSER,
FDLUROLE,
DLCNTRCT,
FDLROUND,
FDLWROUND,
FDLWCCTYP,
FDLSTATUS,
HELDFLG,
LHWKCTR,
LHEVENT,
LHDATEL,
LHTIMEL,
FDLEMSEQNO,
FDLDATETIMEL,
LHEMSEQNO,
LHDATETIMEL,
OBEMSEQNO,
OBDATETIMEL,
ADDRLINE1,
REGION_TERM,
REGION_TO,
FRMREGION,
TOREGION,
DEDDCUTOF,
DEDDDAYS,
TERMSORT,
TERMREGION,
EDDDAYS,
MOBILE,
EMAIL,
EDDCRTDT
)
WITH
  eventclass AS (
  SELECT
    event AS event,
    class AS class
  FROM
    `mpc-dev-459470.DELEVERIES.C_WRK_EM_REF_EVENT_GROUPING`
  WHERE
    trim(class) IN ('LODGE',
      'ONBOARD',
      'SORT',
      'DELIVERY',
      'OTHER',
      'DEDD') ),
  guids AS (
  SELECT
    DISTINCT _BIC_EHGUID
  FROM
    `mpc-dev-459470.PARCEL.FF_O053` e
  WHERE e.EDAP_RECORD_DATE BETWEEN "2019-07-01" and "2019-07-01"
   --and  e. _BIC_EH_CRTDTE BETWEEN "20190301" AND "20190301"
    AND (EXISTS (
      SELECT
        1
      FROM
        eventclass
      WHERE
        e._BIC_EXTEVENT = trim(EVENT))) ), 
  all_events AS (
  SELECT
    _BIC_EHGUID,
    _BIC_EVMGUID,
    EVENT_CD,
    _BIC_EH_CRTDTE,
    _BIC_EH_CRTTME,
    _BIC_EH_TRKCD,
    _BIC_EH_TRK2CD,
    _BIC_BPARTNER,
    _BIC_CUSTREC,
    _BIC_DLVRNTWRK,
    _BIC_LOD_DATE,
    _BIC_MANIFSTID,
    _BIC_MERLOCID,
    _BIC_PCODEFROM,
    _BIC_PRODUCT,
    _BIC_SERVICE,
    _BIC_SNDRPCODE,
    _BIC_SUBPROD,
    _BIC_TIMETABLE,
    _BIC_WCLODGE,
    _BIC_ACC1STDT,
    _BIC_DEL1STDT,
    _BIC_DELDATE,
    _BIC_DRV1STACC,
    _BIC_DRV1STDEL,
    _BIC_PCODEREC,
    _BIC_PCODETO,
    _BIC_RSNCODET2,
    _BIC_STOP_CK,
    _BIC_STOP_DATE,
    _BIC_STOP_TIME,
    _BIC_WC1STACC,
    _BIC_WC1STDEL,
    _BIC_DEVICESRC,
    _BIC_EVNTDTLOC,
    _BIC_EVNTTMLOC,
    _BIC_EXTEVENT,
    _BIC_MSGDTLOC,
    _BIC_MSGTMLOC,
    _BIC_SENDERID,
    _BIC_WCCTYPE,
    _BIC_WORKCTR,
    _BIC_MISEVENT,
    _BIC_EHMSEQNBR,
    _BIC_DATETIMEL,
    _BIC_USERROLE,
    _BIC_CONTRACT,
    _BIC_DEVUSRID,
    _BIC_ROUNDNO,
    _BIC_WCCROUND,
    _BIC_EHADDR1CD,
    _BIC_PROCDATE,
    _BIC_FRMREGION,
    _BIC_TOREGION,
    _BIC_DEDDCUTOF,
    _BIC_DEDDDAYS,
    _BIC_TERMSORT,
    _BIC_EHMOBL_CD,
    _BIC_EHEMAL_CD,
    EDAP_RECORD_DATE
  FROM
    `mpc-dev-459470.PARCEL.FF_O053` e
  WHERE
    _BIC_EHGUID IN (
    SELECT
      *
    FROM
      guids)
    AND  e. EDAP_RECORD_DATE BETWEEN "2019-07-01" and "2019-07-01"
    --AND  e. _BIC_EH_CRTDTE BETWEEN "20181201" AND "20190301"
    AND (EXISTS (
      SELECT
        1
      FROM
        eventclass
      WHERE
        e._BIC_EXTEVENT = trim(EVENT))) )
  -- Article Latest Info
  ,
  last_evt AS (
  SELECT
    *
  FROM (
    SELECT
      _BIC_EHGUID as EH_GUID,
      _BIC_EVMGUID,
      EVENT_CD,
      _BIC_EH_CRTDTE,
      _BIC_EH_CRTTME,
      _BIC_EH_TRKCD,
      _BIC_EH_TRK2CD,
      _BIC_BPARTNER,
      _BIC_CUSTREC,
      _BIC_DLVRNTWRK,
      _BIC_LOD_DATE,
      _BIC_MANIFSTID,
      _BIC_MERLOCID,
      _BIC_PCODEFROM,
      _BIC_PRODUCT,
      _BIC_SERVICE,
      _BIC_SNDRPCODE,
      _BIC_SUBPROD,
      _BIC_TIMETABLE,
      _BIC_WCLODGE,
      _BIC_ACC1STDT,
      _BIC_DEL1STDT,
      _BIC_DELDATE,
      _BIC_DRV1STACC,
      _BIC_DRV1STDEL,
      _BIC_PCODEREC,
      _BIC_PCODETO,
      _BIC_RSNCODET2,
      _BIC_STOP_CK,
      _BIC_STOP_DATE,
      _BIC_STOP_TIME,
      _BIC_WC1STACC,
      _BIC_WC1STDEL,
      _BIC_DEVICESRC,
      _BIC_EVNTDTLOC,
      _BIC_EVNTTMLOC,
      _BIC_EXTEVENT,
      _BIC_MSGDTLOC,
      _BIC_MSGTMLOC,
      _BIC_SENDERID,
      _BIC_WCCTYPE,
      _BIC_WORKCTR,
      _BIC_MISEVENT,
      _BIC_EHMSEQNBR,
      _BIC_DATETIMEL,
      _BIC_USERROLE,
      _BIC_CONTRACT,
      _BIC_DEVUSRID,
      _BIC_ROUNDNO,
      _BIC_WCCROUND,
      _BIC_EHADDR1CD,
      _BIC_FRMREGION,
      _BIC_TOREGION,
      _BIC_DEDDCUTOF,
      _BIC_DEDDDAYS,
      _BIC_TERMSORT,
      _BIC_EHMOBL_CD,
      _BIC_EHEMAL_CD,
      ROW_NUMBER() OVER (PARTITION BY _BIC_EHGUID ORDER BY _BIC_EHMSEQNBR DESC, _BIC_DATETIMEL DESC) AS SEQ
    FROM
      all_events
    WHERE
      EDAP_RECORD_DATE BETWEEN "2019-07-01" and "2019-07-01"
      AND (EXISTS (
        SELECT
          1
        FROM
          eventclass
        WHERE
          _BIC_EXTEVENT = trim(EVENT)
          AND trim(CLASS) = 'DELIVERY')) )
  WHERE
    SEQ = 1 ),
    
  -- EDD Calculation
  calc_edd AS (
  SELECT
    e.*
  FROM (
    SELECT
     _BIC_EHGUID as EH_GUID,
      substr(_BIC_RSNCODET2,0,8) AS EDD,
      _BIC_EVNTDTLOC as EDDCRTDT,
      ROW_NUMBER() OVER (PARTITION BY _BIC_EHGUID ORDER BY _BIC_EHMSEQNBR ASC, _BIC_DATETIMEL ASC) AS SEQ
    FROM
      all_events e
    WHERE
          substr(_BIC_RSNCODET2,0,8) <> ''
					and substr(_BIC_RSNCODET2,0,8) <> '00000000'
  /*    and EXISTS(
      SELECT
        1
      FROM
        eventclass
      WHERE
        e. _BIC_EXTEVENT = trim(EVENT) AND trim(CLASS) IN ('LODGE','SORT'))
        */
        ) e
  WHERE
    seq = 1 ),
  calc_from_region AS (
  SELECT
    *
  FROM (
    SELECT
      _BIC_EHGUID as EH_GUID,
      _BIC_FRMREGION,
      _BIC_TOREGION,
      _BIC_DEDDCUTOF,
      _BIC_DEDDDAYS,
      _BIC_TERMSORT,
      ROW_NUMBER() OVER (PARTITION BY _BIC_EHGUID ORDER BY _BIC_EHMSEQNBR ASC, _BIC_DATETIMEL ASC) AS SEQ
    FROM
      all_events e
    WHERE
      EXISTS(
      SELECT
        1
      FROM
        eventclass
      WHERE
        e._BIC_EXTEVENT = trim(EVENT) AND trim(CLASS) IN ('DEDD')) ) lfphase
  WHERE
    seq = 1 ),
    
  calc_to_term_region AS (
  SELECT
    *
  FROM (
    SELECT
       _BIC_EHGUID as EH_GUID,
      _BIC_FRMREGION,
      _BIC_TOREGION,
      _BIC_DEDDCUTOF,
      _BIC_DEDDDAYS,
      _BIC_TERMSORT,
      ROW_NUMBER() OVER (PARTITION BY _BIC_EHGUID ORDER BY _BIC_EHMSEQNBR DESC, _BIC_DATETIMEL DESC) AS SEQ
    FROM
      all_events e
    WHERE
      EXISTS(
      SELECT
        1
      FROM
        eventclass
      WHERE
        e._BIC_EXTEVENT = trim(EVENT) AND trim(CLASS) IN ('DEDD')) ) lfphase
  WHERE
    seq = 1 )
  -- First Facility Processing Phase
  ,
  first_facproc AS (
  SELECT
    e.*,
    'X' AS FPFSTATUS
  FROM (
    SELECT
      _BIC_EHGUID as EH_GUID,
      _BIC_WORKCTR AS FPFWKCTR,
      _BIC_EXTEVENT AS FPFEVENT,
      _BIC_SENDERID AS FPFSRC,
      _BIC_EVNTDTLOC AS FPFDATEL,
      _BIC_EVNTTMLOC AS FPFTIMEL,
      _BIC_DEVICESRC AS FPFDVSRC,
      _BIC_MSGDTLOC AS FPFMSDTL,
      _BIC_MSGTMLOC AS FPFMSTML,
      _BIC_WCCTYPE AS FPFWCCTYP,
      _BIC_MISEVENT AS FPFMISEVNT,
      ROW_NUMBER() OVER (PARTITION BY _BIC_EHGUID ORDER BY _BIC_EHMSEQNBR ASC, _BIC_DATETIMEL ASC) AS SEQ
    FROM
      all_events e
    WHERE
      EXISTS(
      SELECT
        1
      FROM
        eventclass
      WHERE
        e._BIC_EXTEVENT = trim(EVENT) AND trim(CLASS) IN ('SORT')) ) e
  WHERE
    seq = 1 )
  -- Last Facility Processing Phase
  ,
  last_facproc AS (
  SELECT
    *
  FROM (
    SELECT
       _BIC_EHGUID as EH_GUID,
      _BIC_WORKCTR AS FPWKCTR,
      _BIC_EXTEVENT AS FPEVENT,
      _BIC_SENDERID AS FPSRC,
      _BIC_EVNTDTLOC AS FPDATEL,
      _BIC_EVNTTMLOC AS FPTIMEL,
      _BIC_DEVICESRC AS FPDVSRC,
      _BIC_MSGDTLOC AS FPMSDTL,
      _BIC_MSGTMLOC AS FPMSTML,
      _BIC_WCCTYPE AS FPWCCTYP,
      _BIC_MISEVENT AS FPMISEVNT,
      SUM(1) OVER (PARTITION BY _BIC_EHGUID) AS FPOKCNT,
      ROW_NUMBER() OVER (PARTITION BY _BIC_EHGUID ORDER BY _BIC_EHMSEQNBR DESC, _BIC_DATETIMEL DESC) AS SEQ
    FROM
      all_events e
    WHERE
      EXISTS(
      SELECT
        1
      FROM
        eventclass
      WHERE
        e._BIC_EXTEVENT = trim(EVENT) AND trim(CLASS) IN ('SORT')) )
  WHERE
    seq = 1 )
  -- Onboarding Phase
  ,
  onboard AS (
  SELECT
    *
  FROM (
    SELECT
      _BIC_EHGUID as EH_GUID,
       _BIC_WORKCTR AS OBWKCTR,
      _BIC_EXTEVENT AS OBEVENT,
      _BIC_SENDERID AS OBSRC,
      _BIC_EVNTDTLOC AS OBDATEL,
      _BIC_EVNTTMLOC AS OBTIMEL,
      _BIC_DEVICESRC AS OBUSER,
      _BIC_USERROLE AS OBUROLE,
      _BIC_CONTRACT AS OBCNTRCT,
      'X' AS OBSTATUS,
      _BIC_MSGDTLOC AS OBMSDTL,
      _BIC_MSGTMLOC AS OBMSTML,
      _BIC_EHMSEQNBR AS OBEMSEQNO,
      _BIC_DATETIMEL AS OBDATETIMEL,
      ROW_NUMBER() OVER (PARTITION BY _BIC_EHGUID ORDER BY _BIC_EHMSEQNBR DESC, _BIC_DATETIMEL DESC) AS SEQ
    FROM
      all_events e
    WHERE
      EXISTS(
      SELECT
        1
      FROM
        eventclass
      WHERE
        e._BIC_EXTEVENT = trim(EVENT)
        AND trim(CLASS) IN ('ONBOARD')) ) onb
    -- LEFT JOIN wcc_desc wc ON wc."WORKCNTR" = onb."OBWKCTR"
  WHERE
    seq = 1 )
  -- Fisrt Delivery Phase
  ,
  first_del AS (
  SELECT
    *
  FROM (
    SELECT
      _BIC_EHGUID as EH_GUID,
      _BIC_WORKCTR AS FDLWKCTR,
      _BIC_SENDERID AS FDLSRC,
      _BIC_EXTEVENT AS FDLEVENT,
      _BIC_EVNTDTLOC AS FDLDATEL,
      _BIC_EVNTTMLOC AS FDLTIMEL,
      EVENT_CD AS FDLEVENTCD,
      'X' AS FDLSTATUS,
      _BIC_MSGDTLOC AS FDLMSDTL,
      _BIC_MSGTMLOC AS FDLMSTML,
      _BIC_DEVUSRID AS FDLUSER,
      _BIC_USERROLE AS FDLUROLE,
      _BIC_CONTRACT AS FDLCNTRCT,
      _BIC_ROUNDNO AS FDLROUND,
      _BIC_WCCTYPE AS FDLWCCTYP,
      _BIC_WCCROUND AS FDLWROUND,
      CASE
        WHEN _BIC_EXTEVENT IN ('AFC-ER55', 'AFP-ER55') THEN 'X'
        ELSE NULL
      END AS HELDFLG,
      _BIC_EHMSEQNBR AS FDLEMSEQNO,
      _BIC_DATETIMEL AS FDLDATETIMEL,
      case when    substr(_BIC_RSNCODET2,0,8) <> ''
					and substr(_BIC_RSNCODET2,0,8) <> '00000000' then substr(_BIC_RSNCODET2,0,8) end AS DEDD,
      ROW_NUMBER() OVER (PARTITION BY _BIC_EHGUID ORDER BY _BIC_EHMSEQNBR ASC, _BIC_DATETIMEL ASC) AS SEQ
    FROM
      all_events e
    WHERE
      EXISTS(
      SELECT
        1
      FROM
        eventclass
      WHERE
        e._BIC_EXTEVENT = trim(EVENT) AND trim(CLASS) IN ('DELIVERY')) )
  WHERE
    seq = 1 )
  -- Last Handling Phase
  ,
  last_handle AS (
  SELECT
    *
  FROM (
    SELECT
      _BIC_EHGUID as EH_GUID,
      _BIC_WORKCTR AS LHWKCTR,
      _BIC_EXTEVENT AS LHEVENT,
      _BIC_EVNTDTLOC AS LHDATEL,
      _BIC_EVNTTMLOC AS LHTIMEL,
      _BIC_EHMSEQNBR AS LHEMSEQNO,
      _BIC_DATETIMEL AS LHDATETIMEL,
      ROW_NUMBER() OVER (PARTITION BY _BIC_EHGUID ORDER BY _BIC_EHMSEQNBR DESC, _BIC_DATETIMEL DESC) AS SEQ
    FROM
      all_events e
    WHERE
      EXISTS(
      SELECT
        1
      FROM
        eventclass
      WHERE
        e._BIC_EXTEVENT = trim(EVENT) AND trim(CLASS) IN ('OTHER')) ) lh
    --   LEFT JOIN wcc_desc wc1 ON wc1."WORKCNTR" = lh."LHWKCTR"
  WHERE
    seq = 1 )
    
  ----------------------------- MAIN SELECT STATEMENT ---------------------------------
SELECT
  DISTINCT lart.EH_GUID AS EH_GUID,
  substr(cast(extract(QUARTER from (parse_date('%Y%m%d',lart._BIC_EH_CRTDTE))) as string),1,1) AS CALQUART1,
  lart. _BIC_EH_TRKCD AS ARTICLEID,
  case when lart. _BIC_EH_CRTDTE <> '00000000' then parse_date('%Y%m%d',lart. _BIC_EH_CRTDTE)  end AS  ARTCRTDT,
  case when lart. _BIC_EH_CRTTME <> '000000' then parse_time("%H%M%S",lart. _BIC_EH_CRTTME) end AS  ARTCRTTM ,
  lart. _BIC_EH_TRK2CD  AS  CONSIGNID ,
  lart. _BIC_MANIFSTID  AS  MANIFEST ,
  lart._BIC_MERLOCID  AS  MERLOCID ,
  lart._BIC_PRODUCT  AS  PRODUCT ,
  lart._BIC_SUBPROD  AS  SUBPROD ,
  lart._BIC_SERVICE  AS  SERVICE ,
  lart._BIC_CUSTREC  AS  CUSTREC ,
  lart._BIC_BPARTNER  AS  BPARTNER ,
  case when lart._BIC_LOD_DATE <> '00000000' then parse_date('%Y%m%d',lart._BIC_LOD_DATE)  end AS  LOD_DATE ,
  lart._BIC_DLVRNTWRK  AS  DLVRNTWRK ,
  lart._BIC_PCODEFROM  AS  PCODEFROM ,
  lart._BIC_SNDRPCODE  AS  SNDRPCODE ,
  lart._BIC_TIMETABLE  AS  TIMETABLE ,
  lart._BIC_WCLODGE  AS  WCLODGE ,
  case when lart._BIC_ACC1STDT <> '00000000' then parse_date('%Y%m%d',lart._BIC_ACC1STDT) end AS  ACC1STDT ,
  case when lart._BIC_DEL1STDT <> '00000000' then parse_date('%Y%m%d',lart._BIC_DEL1STDT )  end AS  DEL1STDT ,
  case when lart._BIC_DELDATE <> '00000000' then parse_date('%Y%m%d',lart._BIC_DELDATE)  end AS  DELDATE ,
  lart._BIC_DRV1STACC  AS  DRV1STACC ,
  lart._BIC_DRV1STDEL  AS  DRV1STDEL ,
  lart._BIC_PCODEREC  AS  PCODEREC ,
  case when fdel.DEDD <> '00000000' then parse_date('%Y%m%d',fdel.DEDD)  end AS  DEDD ,
  lart._BIC_STOP_CK  AS  STOP_CK ,
  case when lart._BIC_STOP_DATE <> '00000000' then parse_date('%Y%m%d',lart._BIC_STOP_DATE) end AS  STOP_DATE ,
  case when lart._BIC_STOP_TIME <> '000000' then parse_time("%H%M%S",lart._BIC_STOP_TIME)  end AS  STOP_TIME ,
  lart._BIC_WC1STACC  AS  WC1STACC ,
  lart._BIC_WC1STDEL  AS  WC1STDEL ,
  lart._BIC_PCODETO  AS  PCODETO,
  case when edd. EDD <> '00000000' then  parse_date('%Y%m%d',edd. EDD) end AS EDD,
  -- First Processing
  ffp. FPFWKCTR  AS  FPFWKCTR ,
  ffp. FPFEVENT  AS  FPFEVENT ,
  ffp. FPFSRC  AS  FPFSRC ,
  case when ffp. FPFDATEL <> '00000000' then parse_date('%Y%m%d',ffp. FPFDATEL) end AS  FPFDATEL ,
  case when ffp. FPFTIMEL <> '000000' then parse_time("%H%M%S",ffp. FPFTIMEL) end AS  FPFTIMEL ,
  ffp. FPFDVSRC  AS  FPFDVSRC ,
  case when ffp. FPFMSDTL <> '00000000' then parse_date('%Y%m%d',ffp. FPFMSDTL) end AS  FPFMSDTL ,
  case when ffp. FPFMSTML <> '000000' then parse_time("%H%M%S",ffp. FPFMSTML) end AS  FPFMSTML ,
  ffp. FPFWCCTYP  AS  FPFWCCTYP ,
  ffp. FPFMISEVNT  AS  FPFMISEVNT ,
  ffp. FPFSTATUS  AS  FPFSTATUS,
  -- Last Processing
  lfp. FPEVENT  AS  FPEVENT ,
  lfp. FPWKCTR  AS  FPWKCTR ,
  lfp. FPSRC  AS  FPSRC ,
  case when lfp. FPDATEL <> '00000000' then parse_date('%Y%m%d',lfp. FPDATEL) end AS  FPDATEL ,
  case when lfp. FPTIMEL <> '000000' then parse_time("%H%M%S",lfp. FPTIMEL) end AS  FPTIMEL ,
  lfp. FPDVSRC  AS  FPDVSRC ,
  case when lfp. FPMSDTL <> '00000000' then parse_date('%Y%m%d',lfp. FPMSDTL) end AS  FPMSDTL ,
  case when lfp. FPMSTML <> '000000' then parse_time("%H%M%S",lfp. FPMSTML) end AS  FPMSTML ,
  lfp. FPWCCTYP  AS  FPWCCTYP ,
  lfp. FPMISEVNT  AS  FPMISEVNT ,
  lfp. FPOKCNT  AS  FPOKCNT,
  -- Onboard
  ob. OBWKCTR  AS  OBWKCTR ,
  ob. OBEVENT  AS  OBEVENT ,
  ob. OBSRC  AS  OBSRC ,
  case when ob. OBDATEL <> '00000000' then parse_date('%Y%m%d',ob. OBDATEL) end AS  OBDATEL ,
  case when ob. OBTIMEL <> '000000' then parse_time("%H%M%S",ob. OBTIMEL) end AS  OBTIMEL ,
  ob. OBUSER  AS  OBUSER ,
  ob. OBUROLE  AS  OBUROLE ,
  ob. OBCNTRCT  AS  OBCNTRCT ,
  ob. OBSTATUS  AS  OBSTATUS ,
  case when ob. OBMSDTL <> '00000000' then parse_date('%Y%m%d',ob. OBMSDTL) end AS  OBMSDTL ,
  case when ob. OBMSTML <> '000000' then parse_time("%H%M%S",ob. OBMSTML) end AS  OBMSTML ,
  --First Delivery
  
  fdel. FDLWKCTR  AS  FDLWKCTR ,
  case when fdel. FDLDATEL <> '00000000' then parse_date('%Y%m%d',fdel. FDLDATEL) end AS  FDLDATEL ,
  case when fdel. FDLTIMEL <> '000000' then parse_time("%H%M%S",fdel. FDLTIMEL) end AS  FDLTIMEL ,
  fdel. FDLEVENT  AS  FDLEVENT ,
  fdel. FDLSRC  AS  FDLSRC ,
  fdel. FDLUSER  AS  FDLUSER ,
  fdel. FDLUROLE  AS  FDLUROLE ,
  fdel. FDLCNTRCT  AS  FDLCNTRCT ,
  fdel. FDLROUND  AS  FDLROUND ,
  fdel. FDLWROUND  AS  FDLWROUND ,
  fdel. FDLWCCTYP  AS  FDLWCCTYP ,
  fdel. FDLSTATUS  AS  FDLSTATUS ,
  fdel. HELDFLG  AS  HELDFLG ,
  lhan. LHWKCTR  AS  LHWKCTR ,
  lhan. LHEVENT  AS  LHEVENT ,
  case when lhan. LHDATEL <> '00000000' then parse_date('%Y%m%d',lhan. LHDATEL) end AS  LHDATEL ,
  case when lhan. LHTIMEL <> '000000' then  parse_time("%H%M%S",lhan. LHTIMEL) end  AS  LHTIMEL ,
  fdel. FDLEMSEQNO  AS  FDLEMSEQNO ,
  case when fdel. FDLDATETIMEL <> '00000000000000' then parse_timestamp('%Y%m%d %H%M%S',fdel. FDLDATETIMEL) end AS  FDLDATETIMEL ,
  lhan. LHEMSEQNO  AS  LHEMSEQNO ,
  case when lhan. LHDATETIMEL <> '00000000000000' then  parse_timestamp('%Y%m%d %H%M%S',lhan. LHDATETIMEL) end AS  LHDATETIMEL ,
  ob. OBEMSEQNO  AS  OBEMSEQNO ,
   case when ob. OBDATETIMEL <> '00000000000000' then parse_timestamp('%Y%m%d %H%M%S',ob. OBDATETIMEL) end AS  OBDATETIMEL ,
  lart. _BIC_EHADDR1CD  AS  ADDRLINE1 ,
  '' AS  REGION_TERM ,
  '' AS  REGION_TO ,
  frmreg._BIC_FRMREGION  AS  FRMREGION ,
  totermreg._BIC_TOREGION  AS  TOREGION ,
  totermreg._BIC_DEDDCUTOF  AS  DEDDCUTOF ,
  totermreg._BIC_DEDDDAYS  AS  DEDDDAYS ,
  totermreg._BIC_TERMSORT  AS  TERMSORT ,
  totermreg._BIC_FRMREGION  AS  TERMREGION ,
  frmreg._BIC_DEDDDAYS  AS  EDDDAYS,
  lart._BIC_EHMOBL_CD,
  lart._BIC_EHEMAL_CD,
  case when edd. EDDCRTDT <> '00000000' then parse_date('%Y%m%d',edd. EDDCRTDT) end as EDDCRTDT
FROM
  last_evt lart
LEFT JOIN
  first_del fdel
ON
  lart.EH_GUID  = fdel.EH_GUID 
LEFT JOIN
  first_facproc ffp
ON
  lart.EH_GUID  = ffp. EH_GUID 
LEFT JOIN
  last_facproc lfp
ON
  lart. EH_GUID  = lfp. EH_GUID 
LEFT JOIN
  onboard ob
ON
  lart. EH_GUID  = ob. EH_GUID 
LEFT JOIN
  calc_edd edd
ON
  lart. EH_GUID  = edd. EH_GUID 
LEFT JOIN
  last_handle lhan
ON
  lart. EH_GUID  = lhan. EH_GUID 
LEFT JOIN
  calc_from_region frmreg
ON
  lart. EH_GUID  = frmreg.EH_GUID 
LEFT JOIN
  calc_to_term_region totermreg
ON
  lart. EH_GUID  = totermreg.EH_GUID 
 ;
