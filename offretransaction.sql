

SELECT
    CASE WHEN @ipon_country_code IN ("ES","PT") THEN LPAD(SPLIT(d.prd_item_unique_code,"_")[OFFSET(3)],14,"0")
         WHEN @ipon_country_code IN ("PL","RO") THEN LPAD(SPLIT(d.prd_item_unique_code,"_")[OFFSET(2)],14,"0")
         WHEN @ipon_country_code  IN ("HU")      THEN LPAD(SPLIT(d.prd_item_unique_code,"_")[OFFSET(1)],14,"0")
         END AS product_id,
    d.prd_item_unique_code,
    SPLIT( d.prd_sku_unique_code,"_")[OFFSET(3)] AS prd_sku_code,
    d.prd_sku_unique_code,
    -- h.person_unique_code,
    IF( h.person_unique_code IS NULL , "-1" , TO_HEX(SHA256(h.person_unique_code))) AS client_id,
    d.site_code AS store_id,
    d.basket_unique_code AS transaction_id,
    h.basket_sales_date AS transaction_date,
    CAST(h.basket_sales_dth AS TIMESTAMP) AS transaction_temps,
    s.basket_status_sde AS transaction_status,
    SUM(d.final_amt_w_disc_wo_tax) AS caht,
    -- amt_wo_disc_wo_tax,
    SUM(d.final_amt_w_disc_w_tax) AS cattc,
    SUM(d.nbr_products) AS uvc,
    IF(discount_amt = 0,0,1) AS flag_promotion,
    SUM(discount_amt) AS reward
FROM
  `auchan-country-prod.edm_sales.f_basket_detail` d
INNER JOIN
  `auchan-country-prod.edm_sales.f_basket_header` h
USING
  (basket_unique_code)
LEFT JOIN
  `auchan-country-prod.edm_sales.d_basket_status` s
USING
  (basket_unique_code)
INNER JOIN
    `ard-corp-cfeng-sandbox.axe.nom_FMCG_country` n --this table should be executée before to filter "FMCG"
ON
    d.prd_sku_unique_code = n.prd_sku_unique_code
    -- CASE WHEN @country_codes  IN ("HU")
    -- THEN LPAD(SPLIT(d.prd_item_unique_code ,"_")[OFFSET(1)],14,"0")= n.product_id
    -- AND d.prd_sku_unique_code = n.prd_sku_unique_code
    -- ELSE d.prd_item_unique_code = n.prd_item_unique_code
    -- END
WHERE
  -- CASE WHEN  @country_codes IN ("HU") THEN

------------HUN--------------
--   d.basket_sales_date >= "2021-01-01"
-- AND
--   h.basket_sales_date >= "2021-01-01"


--------------HORS HUN--------------
  DATE(d._PARTITIONTIME) >= "2021-01-01"
AND
  DATE(h._PARTITIONTIME) >= "2021-01-01"


-- AND
--   prd_sku_unique_code = "ES_DWH_-1_639633_2003-04-15"

AND
    CASE WHEN @ipon_country_code  IN ("ES") THEN s.basket_status_code IN ("1")--esp
         WHEN @ipon_country_code  IN ("PL") THEN s.basket_status_code IN ("1","9")--pol
         WHEN @ipon_country_code  IN ("PT") THEN s.basket_status_code IN ("1","3","4","5","7","8","9")--prt
         WHEN @ipon_country_code  IN ("RO") THEN s.basket_status_code IN ("1","2")--rou
         WHEN @ipon_country_code  IN ("HU") THEN s.basket_status_code IN ("Valid")--hun
         END
GROUP BY
    product_id,
    prd_item_unique_code,
    prd_sku_code,
    prd_sku_unique_code,
    client_id,
    store_id,
    transaction_id,
    transaction_date,
    transaction_temps,
    transaction_status,
    flag_promotion

