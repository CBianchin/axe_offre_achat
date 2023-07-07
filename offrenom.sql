

SELECT
    DISTINCT
    -- i.prd_item_code AS product_id,
    -- i.prd_item_unique_code,
    -- p.prd_sku_code,
    f.prd_sku_unique_code,
    f.nomenclature_lvl1_code,
    f.nomenclature_lvl1_sde,
    f.nomenclature_lvl2_code,
    f.nomenclature_lvl2_sde,
    f.nomenclature_lvl3_code,
    f.nomenclature_lvl3_sde,
    f.nomenclature_lvl4_code,
    f.nomenclature_lvl4_sde,
    f.nomenclature_lvl5_code,
    f.nomenclature_lvl5_sde,
    f.nomenclature_lvl6_code,
    f.nomenclature_lvl6_sde,
    n.id_nom_lv1_intl,
    n.ds_nom_lv1_intl,
    n.id_nom_lv2_intl,
    n.ds_nom_lv2_intl,
    n.id_nom_lv3_intl,
    n.ds_nom_lv3_intl,
    n.id_nom_lv4_intl,
    n.ds_nom_lv4_intl,
    n.id_nom_lv5_intl,
    n.ds_nom_lv5_intl,
    n.id_nom_lv6_intl,
    n.ds_nom_lv6_intl,


  --  `auchan-CNT-prod.edm_product.d_product_sku` p

------------------HORS HUN----------------
-- INNER JOIN
--   ( SELECT DISTINCT prd_item_code,prd_item_unique_code,prd_sku_unique_code
--     FROM `auchan-CNT-prod.edm_product.d_product_item` -- vide pour la HUN
--     WHERE
--      item_discontinuation_date is null or item_discontinuation_date >= CURRENT_DATE() ) i
-- USING
--   (prd_sku_unique_code)

--------------------HUN-----------------
-- INNER JOIN
--   ( SELECT DISTINCT prd_item_code,prd_item_unique_code,prd_sku_unique_code, item_creation_date, item_int_date FROM  `auchan-hun-preprod.edm_product.d_product_item` i --HUN
--     WHERE
--      item_discontinuation_date is null or item_discontinuation_date >= CURRENT_DATE() ) i
-- ON p.prd_sku_unique_code = REPLACE(REPLACE(
--     REPLACE(i.prd_sku_unique_code, "MDM", "INFODESK"), SPLIT(i.prd_sku_unique_code, "_")[OFFSET(3)],
--     CAST(CAST(SPLIT(i.prd_sku_unique_code, "_")[OFFSET(3)] as INT) as string)), cast(item_creation_date as string), cast(item_int_date as string))


-- INNER JOIN
  -- (SELECT * FROM
FROM
   `auchan-CNT-prod.edm_product.d_product_nomenclature_flat`  f -- in order to bring nom_flat
-- ON
--   (p.prd_sku_unique_code=f.prd_sku_unique_code)
INNER JOIN
  `auchan-ire-prod.agg_iren_sec.nom_sku_all_""" || param.country_codes || """` n --to filter "FMCG" at nom_intl level.
ON
  CASE WHEN @country_codes  IN ("ES","PT") THEN  CAST(nomenclature_lvl4_code AS INT64) = id_nom_lv1_cty
       WHEN @country_codes IN ("HU")       THEN  SUBSTRING(nomenclature_lvl6_code,- 9) = SUBSTRING(cast(id_nom_lv1_cty AS STRING),-9) --hun lv6
       WHEN @country_codes IN ("PL","RO")  THEN SUBSTR(nomenclature_lvl2_code,-3)||SUBSTR(nomenclature_lvl3_code,-3)||SUBSTR(nomenclature_lvl4_code,-3) = SUBSTRING(cast(id_nom_lv1_cty AS STRING),-9)
--   prd_sku_unique_code = "ES_DWH_-1_639633_2003-04-15"
END

WHERE
  nomenclature_current_flg IS NOT FALSE -- TRUE or NULL (PRT)
AND
  DS_NOM_LV6_INTL="FMCG"
