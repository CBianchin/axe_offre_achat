
SELECT
  DISTINCT
  TO_HEX(SHA256(person_unique_code))  AS client_id,
  cust_segmentation_type_code         AS segmentation_id,
  cust_segmentation_type_sde          AS segmentation_desc,
  cust_segmentation_code              AS segment_id,
  cust_segmentation_sde               AS segment_desc,
  cust_segmentation_validity_start_date,
  cust_segmentation_validity_end_date,
  ctry_code                           AS country
FROM `auchan-CNT-prod.edm_customer.d_customer_segmentation`
WHERE cust_segmentation_validity_start_date >= "2021-01-01"
AND cust_segmentation_type_sde IN ("RFMR","PRICE_SENSITIVITY","CHURN","LIFESTAGE_LIGHT","RFMR_WEB","RFMR_360");
-- RFMR -> RFMR des clients pour les achats en physiques
-- RFMR_WEB -> RFMR des clients pour les achats web
-- RFMR_360 -> combinaison des 2 autres
