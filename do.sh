#!/bin/bash

# achatsallextract offremargin offretransaction offrenom offresitboxsize  offreseg offreseg
# marge\|magasins_typologie\|nom_FMCG\|sites\|segmentation\|produits\|transaction'

# gcloud config set project ard-corp-cfeng-sandbox
# sed "s/_CNT/_${3}/g" "s/_country/_${1}/g" offremargin.sql > current_query.sql
# bq --location EU query --max_rows=0 --replace --use_legacy_sql=false --parameter="country_codes:ARRAY<INT64>:[${2}]"  --parameter="ipon_country_code:STRING:${3}" --destination_table "axe.marge_$1" "$(cat current_query.sql)"
# rm current_query.sql


gcloud config set project ard-corp-cfeng-sandbox
sed  "s/_country/_${1}/g" offretransaction.sql > current_query.sql
bq --location EU query --max_rows=0 --replace --use_legacy_sql=false --parameter="country_codes:ARRAY<INT64>:[${2}]"  --parameter="ipon_country_code:STRING:${3}" --destination_table "axe.transaction_$1" "$(cat current_query.sql)"
rm current_query.sql