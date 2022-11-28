jar='./target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
data_dir='/mnt/data/tokenjoin/serialized/'
log_dir='./execution/logs/threshold_verification/'

datasets=('yelp' 'gdelt' 'enron' 'flickr' 'dblp' 'mind')
similarities=('jaccard' 'jaccard' 'jaccard' 'edit' 'edit' 'edit')

for i in "${!datasets[@]}"; do
   name=${datasets[i]}
   similarity=${similarities[i]}
   logFile="$log_dir$name.log"
   inputFile="$data_dir$name""_40.txt"
   
   java -Xms70g -Xmx70g -jar "${jar}" -s "${similarity}" -l "${logFile}" -i "${inputFile}" -d 0.8 -m "TJV" -v 0
done
