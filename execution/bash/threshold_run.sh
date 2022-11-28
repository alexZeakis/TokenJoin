jar='./target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
data_dir='/mnt/data/tokenjoin/serialized/'
log_dir='./execution/logs/threshold_threshold/'

datasets=('yelp' 'gdelt' 'enron' 'flickr' 'dblp' 'mind')
similarities=('jaccard' 'jaccard' 'jaccard' 'edit' 'edit' 'edit')
models=('SM' 'TJ' 'TJP' 'TJPJ')
deltas=(0.95 0.90 0.85 0.80 0.75 0.70 0.65 0.60)

for i in "${!datasets[@]}"; do
   name=${datasets[i]}
   similarity=${similarities[i]}
   logFile="$log_dir$name.log"
   inputFile="$data_dir$name""_40.txt"
   
   for delta in "${deltas[@]}"; do
        for model in "${models[@]}"; do
           java -Xms70g -Xmx70g -jar "${jar}" -s "${similarity}" -l "${logFile}" -i "${inputFile}" -d "${delta}" -m "${model}" -v 0
        done
   done
done
