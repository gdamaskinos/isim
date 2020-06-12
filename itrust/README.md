# I-Trust: Trust predictor for Online Social Networks

## Deploy

The following steps evaluate _I-TRUST_ on the Wikipedia adminship election dataset with a test set of 100 ratings.

1. Setup
    * Ubuntu 18.04.3 LTS
    * Python 3.7
    * Java 1.8.0_77
2. Download and extract the [Wikipedia adminship election dataset](https://snap.stanford.edu/data/wikiElec.ElecBs3.txt.gz)
3. Parse dataset: 
    ```
    python ../parsers/wikiParser.py wikiElec.ElecBs3.txt dataset.csv
    bash ../parsers/splitter.sh dataset.csv ./ 100
    ```
4. Deploy _I-TRUST_
    ```
    javac Itrust.java
    java Itrust trainingSet.csv testSet.csv
    ```



