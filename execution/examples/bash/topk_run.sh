java -jar ./target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c ./execution/examples/configs/yelp_config.json -s jaccard -t topk -m experiment -i ./data/ -l ./execution/examples/logs/
java -jar ./target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c ./execution/examples/configs/gdelt_config.json -s jaccard -t topk -m experiment -i ./data/ -l ./execution/examples/logs/
java -jar ./target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c ./execution/examples/configs/enron_config.json -s jaccard -t topk -m experiment -i ./data/ -l ./execution/examples/logs/
java -jar ./target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c ./execution/examples/configs/flickr_config.json -s edit -t topk -m experiment -i ./data/ -l ./execution/examples/logs/
java -jar ./target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c ./execution/examples/configs/dblp_config.json -s edit -t topk -m experiment -i ./data/ -l ./execution/examples/logs/
java -jar ./target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c ./execution/examples/configs/mind_config.json -s edit -t topk -m experiment -i ./data/ -l ./execution/examples/logs/
