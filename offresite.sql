
with Site_Open_Histo as
  ( select distinct site_unique_code
    from `auchan-CNT-prod.edm_site.d_site_lifecycle`
    --ne pas prendre de current flag ou flag suppr pour avoir tout l'histo
    where site_status_code in ('OPE')
    and (cast(site_status_end_date as date ) >= '2021-01-01' or  site_status_end_date is null)
  )
  --status open depuis 2021

  select
    Site_Open_Histo.site_unique_code --signifie que le site est présent dans le WITH, donc site avec un statut ouvert au moins une fois depuis 2021
    ,SITE_CURRENT.*EXCEPT(site_unique_code,site_status_code,site_status_sde,site_status_start_date,site_type_code) -- toutes les infos sites en valeur courante


  from Site_Open_Histo
  inner join
  (
          SELECT distinct --'fra' as Country,
          site.site_unique_code,
          site.site_code,
          site.ctry_code,
          SPLIT(site.site_unique_code,'_')[SAFE_ORDINAL(3)] as Site_Type_Key,
          type.site_type_code,
          type.site_type_sde
          ,descr.site_sde
          ,format.site_format_code,
          format.site_format_sde,
          surface.surface_code,
          surface.surface_value,
          address.site_address_zip_code,
          address.site_address_city,
          address.site_geo_coordinate_x_value,
          address.site_geo_coordinate_y_value,
          address.coordinate_type_code,
          lifecycle.site_status_code,
          lifecycle.site_status_sde,
          cast (lifecycle.site_status_start_date as date ) as site_status_start_date
          ,affil.affiliation_type_code


      -- valeur courante
          FROM `auchan-CNT-prod.edm_site.d_site` site
          left join  `auchan-CNT-prod.edm_site.d_site_type` type on site.site_unique_code = type.site_unique_code and site_type_current_flg is not false and type.is_deleted is not true  and (site_type_end_date is null or date(site_type_end_date) > current_date)
          left join  `auchan-CNT-prod.edm_site.d_site_description` descr on site.site_unique_code = descr.site_unique_code and site_sde_current_flg is not false and descr.is_deleted is not true and (site_sde_end_date is null or date(site_sde_end_date) > current_date)
          left join  `auchan-CNT-prod.edm_site.b_site_format_link` linkformat on site.site_unique_code = linkformat.site_unique_code and site_format_current_flg is not false and linkformat.is_deleted is not true  and (site_format_end_date is null or date(site_format_end_date) > current_date)
          left join  `auchan-CNT-prod.edm_site.d_site_format` as format on linkformat.site_format_unique_code = format.site_format_unique_code -- pas d'historisation dans cette table
          left join `auchan-CNT-prod.edm_site.d_affiliation_type` affil on site.site_unique_code = affil.site_unique_code and affiliation_type_current_flg is not false and affil.is_deleted is not true and (affiliation_type_end_date is null or date(affiliation_type_end_date) > current_date)
          left join  `auchan-CNT-prod.edm_site.d_site_lifecycle` lifecycle on site.site_unique_code = lifecycle.site_unique_code and site_status_current_flg is not false and lifecycle.is_deleted is not true and (site_status_end_date is null or date(site_status_end_date) > current_date)

          LEFT JOIN `auchan-CNT-prod.edm_site.d_site_activity_surface` surface on site.site_unique_code = surface.site_unique_code and surface.is_deleted is not false
          LEFT JOIN `auchan-CNT-prod.edm_site.d_site_address` address on  site.site_unique_code = address.site_unique_code AND address.site_address_current_flg is not false
          where site_del_flg is not true
          -- and affil.affiliation_type_code not in ('FRA')
          -- and format.site_format_code NOT IN ("RES", "EXP")

  )SITE_CURRENT
  ON SITE_CURRENT.site_unique_code = Site_Open_Histo.site_unique_code
