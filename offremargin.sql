

WITH agg_products as (

SELECT
DISTINCT
    products.* EXCEPT (id_sku),
    s.id_sku
FROM
    `auchan-ire-prod.agg_iren_sec.products_CNT`  products
INNER JOIN
    `auchan-ire-prod.raw_gps_sec.LU_SKU` s USING (SID_SKU)
WHERE
    products.sid_country IN UNNEST(@country_codes)
),

MONTH_BY_MONTH_SALES AS (
SELECT
    sales.sid_country,
    products.id_sku,
    sales.id_month_sld,
    sales.sid_sku,
    sales.sid_ean,
    sales.sid_site,
    CONCAT(ID_CODE_EAN,ID_KEY_EAN )                                                                                     AS EAN,
    DS_EAN,
    SUM(F_SAL_AMT_WO_TAX )                                                                                              AS turnover_wo_taw,
    SUM(ID_FLG_MRG * F_SAL_AMT_WO_TAX) # vente
        - SUM(ID_FLG_MRG * coalesce(F_PUR_SUP_CLC, 0)) # prix achat
        + SUM(ID_FLG_MRG * coalesce(F_PUR_NET_DIS_RBT, 0))                                    # remise arrière
                                                                                                                        AS marge_3net,

   SUM(ID_FLG_MRG * F_SAL_AMT_WO_TAX)                                                                                   AS turnover_margeable,
FROM
    `auchan-ire-prod.agg_iren_sec.sales_CNT`  sales
LEFT JOIN
    (SELECT * FROM agg_products WHERE TO_KEEP is true) products
USING
    (sid_sku)
LEFT JOIN
    `auchan-ire-prod.agg_iren_sec.nom_sku_all_CNT` n
ON
     products.SID_NOM_CTY=n.SID_NOM_CTY
AND
     products.sid_country=n.sid_country

WHERE
      SUBSTR(CAST(ID_MONTH_SLD AS STRING),1,4) >= "2021"
AND
      id_nom_lv6_intl IN ("000004")--filtre DPH
-- AND
--   id_sku is not null
-- AND sid_sku = "FR_FR297102_GPS"
GROUP BY
    sid_country,
    id_sku,
    id_month_sld,
    sid_sku,
    sid_ean,
    sid_site,
    EAN,
    DS_EAN
)

SELECT
    ID_MONTH_SLD,
    SUBSTR(sid_country,0,2)                                                                                             AS COUNTRY,
    --null as SUPPLIER,
    id_sku                                                                                                              AS prd_sku_code,
    EAN,
    safe_divide(SUM(marge_3net ) , SUM(turnover_margeable)) * SUM(turnover_wo_taw)                                      AS marge_3net,
FROM
    MONTH_BY_MONTH_SALES
-- where DS_SUPPLIER_BRAND = "ALPRO" --and MONTH_BY_MONTH_SALES.EAN = "5411188132226"
GROUP BY
    ID_MONTH_SLD,
    COUNTRY,
    prd_sku_code,
    EAN

