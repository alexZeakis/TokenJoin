# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <data_dir>"
  exit 1
fi

# Assign the command-line arguments to variables
data_dir="$1"

jar='./target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
log_dir='./execution/logs/threshold_scalability/'

datasets=('yelp' 'gdelt' 'enron' 'flickr' 'dblp' 'mind')
similarities=('jaccard' 'jaccard' 'jaccard' 'edit' 'edit' 'edit')
models=('SM' 'TJ' 'TJP' 'TJPJ')
parts=(20 40 60 80 100)

for i in "${!datasets[@]}"; do
   name=${datasets[i]}
   similarity=${similarities[i]}
   logFile="$log_dir$name.log"
  
   for part in "${parts[@]}"; do
        inputFile="$data_dir$name""_"""$part".txt"
        for model in "${models[@]}"; do
           java -Xms70g -Xmx70g -jar "${jar}" -s "${similarity}" -l "${logFile}" -i "${inputFile}" -d 0.8 -m "${model}" -v 0
        done
   done
done
