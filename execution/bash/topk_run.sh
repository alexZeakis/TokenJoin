jar='./target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
data_dir='/mnt/data/tokenjoin/serialized/'
log_dir='./execution/logs/topk_k/'

datasets=('yelp' 'gdelt' 'enron' 'flickr' 'mind')
similarities=('jaccard' 'jaccard' 'jaccard' 'edit' 'edit')
models=('SMK' 'TJK' 'FJK')
ks=(500 1000 5000 10000)

for i in "${!datasets[@]}"; do
   name=${datasets[i]}
   similarity=${similarities[i]}
   logFile="$log_dir$name.log"
   inputFile="$data_dir$name""_100_topk.txt"
   
   for k in "${ks[@]}"; do
        for model in "${models[@]}"; do
           java -Xms70g -Xmx70g -jar "${jar}" -s "${similarity}" -l "${logFile}" -i "${inputFile}" -k "${k}" -m "${model}" -v 0
        done
   done
done


datasets=('dblp')
similarities=('edit')
models=('SMK' 'TJK' 'FJK')
ks=(50 100 500 1000)

for i in "${!datasets[@]}"; do
   name=${datasets[i]}
   similarity=${similarities[i]}
   logFile="$log_dir$name.log"
   inputFile="$data_dir$name""_100_topk.txt"
   
   for k in "${ks[@]}"; do
        for model in "${models[@]}"; do
           java -Xms70g -Xmx70g -jar "${jar}" -s "${similarity}" -l "${logFile}" -i "${inputFile}" -k "${k}" -m "${model}" -v 0
        done
   done
done
