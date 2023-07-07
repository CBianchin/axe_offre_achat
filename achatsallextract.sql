

WITH agg_products as (

SELECT
DISTINCT
products.* EXCEPT (id_sku),s.id_sku
FROM  `auchan-ire-prod.agg_iren_sec.products_CNT`  products
INNER JOIN `auchan-ire-prod.raw_gps_sec.LU_SKU` s USING (SID_SKU)
WHERE products.sid_country IN UNNEST(@country_codes)
),

rl_suppliers AS (
SELECT
  sid_sku,
  sid_ean,
  STRING_AGG(DISTINCT cast( ds_activity_department AS string), "|") AS ds_activity_department,
  STRING_AGG(DISTINCT cast( id_activity_department AS string), "|") AS id_activity_department
FROM `agg_iren_sec.sales_CNT`
JOIN `agg_iren_sec.suppliers_CNT`
USING( sid_activity_department)
GROUP BY sid_sku,sid_ean
),




MONTH_BY_MONTH_SALES AS (
SELECT
sales.sid_country,
products.id_sku AS prd_sku_code,
sales.id_month_sld,
sales.sid_sku,
sales.sid_ean,
sales.sid_site,
ID_SUPPLIER_HOLDING,
DS_SUPPLIER_HOLDING,
products.DS_SUPPLIER_BRAND,
rl_suppliers.ds_activity_department,

CONCAT(ID_CODE_EAN,ID_KEY_EAN ) AS EAN,
DS_EAN,

    ID_NOM_LV1_cty,
    ID_NOM_LV2_cty,
    ID_NOM_LV3_cty,
    ID_NOM_LV4_cty,
    DS_NOM_LV1_cty,
    DS_NOM_LV2_cty,
    DS_NOM_LV3_cty,
    DS_NOM_LV4_cty,
    ID_NOM_LV1_INTL,
    ID_NOM_LV2_INTL,
    ID_NOM_LV3_INTL,
    ID_NOM_LV4_INTL,
    DS_NOM_LV1_INTL,
    DS_NOM_LV2_INTL,
    DS_NOM_LV3_INTL,
    DS_NOM_LV4_INTL,
    DS_NOM_LV5_INTL,
    DS_NOM_LV6_INTL,
sum(F_SAL_AMT) as cattc,
sum(F_SAL_AMT_WO_TAX ) as turnover_wo_taw,
COALESCE(ANY_value(F_EXCHANGE_RAT_EUR), 1) as CONVERSION_RATE,
SUM(ID_FLG_MRG * F_SAL_AMT_WO_TAX) # vente
    - SUM(ID_FLG_MRG * coalesce(F_PUR_SUP_CLC, 0)) # prix achat
    as marge_2net,
    SUM(ID_FLG_MRG * F_SAL_AMT_WO_TAX) # vente
    - SUM(ID_FLG_MRG * coalesce(F_PUR_SUP_CLC, 0)) # prix achat
    +SUM(ID_FLG_MRG * coalesce(F_PUR_NET_DIS_RBT, 0)) # remise arrière
    as marge_3net,
    SUM(ID_FLG_MRG * F_SAL_AMT_WO_TAX) # vente
    - SUM(ID_FLG_MRG * coalesce(F_PUR_SUP_CLC, 0)) # prix achat
    +SUM(ID_FLG_MRG * coalesce(F_PUR_NET_DIS_RBT, 0)) # remise arrière
    + SUM(ID_FLG_MRG * coalesce(CAST(F_RBT_4NET_POS AS float64),0)) # remise mandat financier
    -SUM(ID_FLG_MRG * coalesce(CAST(F_RBT_4NET_NGV AS float64),0))
    as marge_4net,
    SUM(ID_FLG_MRG * F_SAL_AMT_WO_TAX) # vente
    - SUM(ID_FLG_MRG * coalesce(F_PUR_SUP_CLC, 0)) # prix achat
    +SUM(ID_FLG_MRG * coalesce(F_PUR_NET_DIS_RBT, 0)) # remise arrière
    + SUM(ID_FLG_MRG * coalesce(CAST(F_RBT_4NET_POS AS float64),0)) # remise mandat financier
    -SUM(ID_FLG_MRG * coalesce(CAST(F_RBT_4NET_NGV AS float64),0)) # gagnotage non financé
    + SUM(ID_FLG_MRG * coalesce(F_RBT_5NET_MRG,0)) AS marge_magasin,
   sum(ID_FLG_MRG * F_SAL_AMT_WO_TAX) as turnover_margeable,
      sum(F_QTY_SLD) AS QTY_SLD
      from `auchan-ire-prod.agg_iren_sec.sales_CNT`  sales
--join `auchan-ire-prod.agg_iren_sec.sites_CNT`
--using (sid_site,sid_site_country)
LEFT JOIN
      (select * from agg_products where TO_KEEP is true) products
USING
      (sid_sku)
LEFT JOIN auchan-ire-prod.agg_iren_sec.exchange_rates_CNT rates on sales.ID_MONTH_SLD=CAST(rates.ID_MONTH as INT64)
AND  CASE WHEN sales.SID_COUNTRY="RS_GPS" THEN "RU_GPS" ELSE sales.SID_COUNTRY END =
     CASE WHEN rates.ID_CURRENCY ="UAH" THEN "UA_GPS"
          WHEN rates.ID_CURRENCY ="RUB" THEN "RU_GPS"
          WHEN rates.ID_CURRENCY ="HUF" THEN "HU_GPS"
          WHEN rates.ID_CURRENCY ="PLN" THEN "PL_GPS"
          WHEN rates.ID_CURRENCY ="RON" THEN "RO_GPS"
          END
LEFT JOIN
  `auchan-ire-prod.agg_iren_sec.nom_sku_all_CNT` n on products.SID_NOM_CTY=n.SID_NOM_CTY and products.sid_country=n.sid_country
LEFT JOIN
  rl_suppliers --récupérer le fournisseur d'achats (! un sid_sku peut contenir plusieurs fournisseurs d'achats )
On
  sales.sid_ean=rl_suppliers.sid_ean and sales.sid_sku=rl_suppliers.sid_sku
WHERE
  SUBSTR(CAST(ID_MONTH_SLD AS STRING),1,4) >= "2021"
AND
  SAFE_CAST(ID_NOM_LV4_INTL as int64) in (248,250,8232,8327)--filtre DPH
-- AND
--   id_sku is not null
-- AND sid_sku = "FR_FR297102_GPS"
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30

)
-- SELECT * FROM (
select
    -- SUBSTR(CAST(ID_MONTH_SLD AS STRING),1,4) as year,
    ID_MONTH_SLD,
    SUBSTR(sid_country,0,2) as COUNTRY,
   -- CHANNEL,
    ID_SUPPLIER_HOLDING as HOLDING_ID,
    DS_SUPPLIER_HOLDING as HOLDING,
    DS_SUPPLIER_BRAND,
    ds_activity_department AS fournisseurs_achats,
    --null as SUPPLIER,
    id_sku AS SKU_ID,
    EAN,
    DS_EAN as DESCRIPTION,
    -- ID_NOM_LV1_cty as ID_NOM_LV1,
    -- ID_NOM_LV2_cty as ID_NOM_LV2,
    -- ID_NOM_LV3_cty as ID_NOM_LV3,
    -- ID_NOM_LV4_cty as ID_NOM_LV4,
    DS_NOM_LV1_cty as DS_NOM_LV1,
    DS_NOM_LV2_cty as DS_NOM_LV2,
    DS_NOM_LV3_cty as DS_NOM_LV3,
    DS_NOM_LV4_cty as DS_NOM_LV4,
    -- ID_NOM_LV1_INTL,
    -- ID_NOM_LV2_INTL,
    -- ID_NOM_LV3_INTL,
    -- ID_NOM_LV4_INTL,
    DS_NOM_LV1_INTL,
    DS_NOM_LV2_INTL,
    DS_NOM_LV3_INTL,
    DS_NOM_LV4_INTL,
    DS_NOM_LV5_INTL,
    DS_NOM_LV6_INTL,
    -- Parse_date("%Y%m%d",CONCAT (ID_MONTH_SLD,01)) AS Periode,
    -- format_Datetime("%B", Parse_date("%Y%m%d",CONCAT (ID_MONTH_SLD,01))) AS Month,
    sum(turnover_wo_taw) AS Turnover,
    sum(turnover_wo_taw*CONVERSION_RATE) AS TO_eur,
    sum(cattc) AS CATTC,
    sum(cattc*CONVERSION_RATE) AS CATTC_eur,
--    sum( MARGE_4NET*CONVERSION_RATE) AS Margin_4Net_eur,
-- sum(MARGE_3NET*CONVERSION_RATE) AS Margin_3Net_eur,
--  sum(marge_magasin*CONVERSION_RATE) AS Margin_5Net_eur,
 safe_divide(sum(marge_2net ) , sum(turnover_margeable)) * SUM(turnover_wo_taw)  as marge_2net,
    safe_divide(sum(marge_3net ) , sum(turnover_margeable)) * SUM(turnover_wo_taw) as marge_3net,
    safe_divide(sum(marge_4net ) , sum(turnover_margeable)) * SUM(turnover_wo_taw) as marge_4net,
    safe_divide(SUM(marge_magasin) , sum(turnover_margeable)) * SUM(turnover_wo_taw) as marge_5net,
 safe_divide(sum(marge_2net*CONVERSION_RATE ) , sum(turnover_margeable*CONVERSION_RATE)) * SUM(turnover_wo_taw*CONVERSION_RATE)  as marge_2net_eur,
    safe_divide(sum(marge_3net*CONVERSION_RATE ) , sum(turnover_margeable*CONVERSION_RATE)) * SUM(turnover_wo_taw*CONVERSION_RATE) as marge_3net_eur,
    safe_divide(sum(marge_4net *CONVERSION_RATE) , sum(turnover_margeable*CONVERSION_RATE)) * SUM(turnover_wo_taw*CONVERSION_RATE) as marge_4net_eur,
    safe_divide(SUM(marge_magasin*CONVERSION_RATE) , sum(turnover_margeable*CONVERSION_RATE)) * SUM(turnover_wo_taw*CONVERSION_RATE) as marge_5net_eur,

    sum(QTY_SLD) as Qty_sold,
    -- CONVERSION_RATE
   -- RC_VMH,
   -- RC_TURNOVER,
   -- RC_MARGIN,
--from nego join
FROM
MONTH_BY_MONTH_SALES
--using(sku_id)
-- where DS_SUPPLIER_BRAND = "ALPRO" --and MONTH_BY_MONTH_SALES.EAN = "5411188132226"
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19--,20,21,22,23,24,25,26,27
--order by DS_SUPPLIER_BRAND
-- ) WHERE marge_2net IS NOT NULL --(need to check if this filter is needed as we can not calculate the margin...)

