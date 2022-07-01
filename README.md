# TokenJoin

## Overview

TokenJoin is an efficient method for solving the Fuzzy Set Similarity Join problem. It relies only on tokens and their defined _utilities_, avoiding pairwise comparisons between elements. It is submitted to the International Conference on Very Large Databases (VLDB). This is the repository for the source code.

## Documentation

Javadoc is available [here](https://alexzeakis.github.io/TokenJoin/).

## Usage

**Step 1**. Download or clone the project:
```sh
$ git clone https://github.com/alexZeakis/TokenJoin.git
```

**Step 2**. Open terminal inside root folder and install by running:
```sh
$ mvn install
```
**Step 3**. Edit the parameters in the corresponding config file.

**Step 4** Run the executable:
```sh
$ java -jar target/tokenjoin-0.0.1-SNAPSHOT-jar-with-dependencies.jar --config <config_file> --similarity <similarity> --type <type> --mode <mode> --input <input_dir> --log <log_dir>
```

- Similarity values: ["jaccard", "edit"]
- Type values: ["threshold", "topk", "threshold_scalability", "threshold_verification", "threshold_threshold" ]
- Mode values: ["experiment"]
- Input directory: The directory of the datasets
- Log directory: the directory that the logs will be stored

The selected combination should be included in the corresponding config file.

## Experiment execution

There are 4 groups of experiments conducted in TokenJoin. To reproduce the experiments mentioned in the paper, run the following commands.

- Comparing ThresholdJoin methods based on varying thresholds.
```sh
$ ./execution/examples/bash/threshold_run.sh
```

- Comparing ThresholdJoin methods based on varying dataset sizes.
```sh
$ ./execution/examples/bash/scalability_run.sh
```

- Comparing Verification methods in ThresholdJoin.
```sh
$ ./execution/examples/bash/verification_run.sh
```

- Comparing TopkJoin methods based on varying k.
```sh
$ ./execution/examples/bash/topk_run.sh
```
These are wrapper scripts for the 6 individual calls to the 6 datasets.

Notice that in the `execution/examples/` we have smaller collection sizes and lighter JVM specifications. To run the original experimens, follow the same instructions but execute the corresponding scripts in `execution/experiments/`. Also notice that the existing logs inside `execution/experiments/` contain the original results and any new execution will be appended to the end of the file.

## Construction of plots

For each experiment a separate python script exists to analyse the results from the corresponding log file. The existing plots can be seen in `execution/experiments/plots/` and can be reproduced by running the following commands:

- Threshold - Threshold
```sh
$ python execution/python/Threshold.py execution/experiments/
```

- Threshold - Scalability
```sh
$ python execution/python/Scalability.py execution/experiments/
```

- Threshold - Verification
```sh
$ python execution/python/Verification.py execution/experiments/
```

- Topk 
```sh
$ python execution/python/Topk.py execution/experiments/
```



## Datasets
We have used six real-world datasets:

- [Yelp](https://www.yelp.com/dataset): 160,016 sets extracted from the Yelp Open Dataset. Each set refers to a business. Its elements are the categories associated to it.

- [GDELT](https://www.gdeltproject.org/data.html): 500,000 randomly selected sets from January 2019 extracted from the GDELT Project. Each set refers to a news article. Its elements are the themes associated with it. Themes are hierarchical. Each theme is represented by a string concatenating all themes from it to the root of the hierarchy.

- [Enron](https://www.cs.cmu.edu/~enron): 517,431 sets, each corresponding to an email message. The elements are the words contained in the message body.

- [Flickr](https://yahooresearch.tumblr.com/post/89783581601/one-hundred-million-creative-commons-flickr-images-for): 500,000 randomly selected images from the Flickr Creative Commons dataset. Each set corresponds to a photo. The elements are the tags associated to that photo.

- [DBLP](https://dblp.uni-trier.de/xml): 500,000 publications from the DBLP computer science bibliography. Each set refers to a publication. The elements are author names and words in the title.

- [MIND](https://msnews.github.io): 123,130 articles from the MIcrosoft News Dataset. Each set corresponds to an article. The elements are the words in its abstract.

The preprocessed versions of the datasets used in the experiments can be found [here](https://drive.google.com/drive/folders/1u9ixJM25koPkHi8FJ0atrHL1WcE8dtLw?usp=sharing).

