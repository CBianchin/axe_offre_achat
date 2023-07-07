    DESTINATION_BUCKET='axe_strat'
    PROJECT='ard-corp-cfeng-sandbox'
    DATASET='axe'
    # TABLE='nom_FMCG_esp'

    # for TABLE in $(bq ls --max_results=1000 $DATASET | grep TABLE | grep 'marge\|magasins_typologie\|nom_FMCG\|sites\|segmentation\|produits\|transaction'| grep -v all | awk '{print $1}'); do

    for TABLE in $(bq ls --max_results=1000 $DATASET | grep TABLE | grep 'marge'| grep -v all | awk '{print $1}'); do
        echo ${TABLE}
    


        bq extract \
        --destination_format 'CSV' \
        --compression 'GZIP' \
        --field_delimiter '|' \
        --print_header=False \
        $PROJECT:$DATASET.$TABLE \
        gs://$DESTINATION_BUCKET/"$TABLE"_temp/"$TABLE"_*.csv.gz

        i=0
        while ((`gsutil ls gs://$DESTINATION_BUCKET/"$TABLE"_temp/ | wc -l`>0))
        do
            file_to_concatenate=`gsutil ls gs://$DESTINATION_BUCKET/"$TABLE"_temp/ | head -n 32 | xargs`
            gsutil compose $file_to_concatenate gs://$DESTINATION_BUCKET/"$TABLE"_temp2/"$TABLE"_$i.csv.gz
            gsutil -m rm $file_to_concatenate
            i=$((i+1))
        done

        i=1
        while ((`gsutil ls gs://$DESTINATION_BUCKET/"$TABLE"_temp2/ | wc -l`>0))
        do
            file_to_concatenate=`gsutil ls gs://$DESTINATION_BUCKET/"$TABLE"_temp2/ | head -n 32 | xargs`
            gsutil compose $file_to_concatenate gs://$DESTINATION_BUCKET/"$TABLE"_temp3/"$TABLE"_$i.csv.gz
            gsutil -m rm $file_to_concatenate
            i=$((i+1))
        done

        header=`bq show --schema $PROJECT:$DATASET.$TABLE | jq '.[].name' |xargs | tr ' ' '|'`

        echo $header | gzip -c | gsutil cp - gs://$DESTINATION_BUCKET/"$TABLE"_temp3/"$TABLE"_0.csv.gz

        gsutil compose gs://$DESTINATION_BUCKET/"$TABLE"_temp3/*.csv.gz gs://$DESTINATION_BUCKET/"$TABLE".csv.gz

        gsutil -m rm gs://$DESTINATION_BUCKET/"$TABLE"_temp3/*.csv.gz
    
    done