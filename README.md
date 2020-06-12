# SwIFT: Scalable Incremental Flexible Temporal recommender

Author: Georgios Damaskinos (georgios.damaskinos@gmail.com)

_SwIFT_ is a recommender introduced in [Capturing the Moment: Lightweight Similarity Computations](https://ieeexplore.ieee.org/document/7930022/) that employs a new similarity metric, namely _I-SIM_.
A secondary application of _I-SIM_, namely _I-TRUST_ (presented in the same paper), can be found [here](itrust/).


## Quick Start

The following steps evaluate _SwIFT_ on the MovieLens100K dataset with a test set of 100 ratings.

1. Setup
    * Ubuntu 18.04.3 LTS
    * Python 3.7
    * Java 1.8.0_77
    * spark-2.4.5-bin-hadoop2.7
    * apache-cassandra-3.11.6
    * ```sudo apt-get install bc```
    * Export variables:
      * JAVA_HOME
      * SPARK_HOME
      * CASSANDRA_HOME
    * Setup cluster: 
    ```
    cd utils/
    RUN_TESTS=1 bash deploy_cluster.sh STANDALONE /path/to/spark_localdir/
    ```
    * Shutdown cluster: 
    ```
    cd utils/
    bash stop_cluster.sh STANDALONE
    ```
2. Download and extract the [MovieLens100K dataset](http://files.grouplens.org/datasets/movielens/ml-100k.zip)
3. Parse dataset: 
    ```
    python parsers/movielensParser.py ml-100k/u.data dataset.csv
    bash parsers/splitter.sh dataset.csv ./ 100
    ```
4. Deploy _SwIFT_
    ```
    bash local_deploy.sh trainingSet.csv testSet.csv 5 10 0 100 1 9995 /path/to/log
    ```

## Structure

_SwIFT_ consists of two main components:

* [Frontend](frontend.py)
  * Accumulates the ratings in microbatches and sends them to the backend
  * Provides recommendations to users and computes CTR and recall 
* [Backend](backend.py)
  * Bootstraps (if necessary) the database
  * Incrementally updates the database given the microbatches that the frontend sends
  * Logs latency measurements and computes the RMSE  


More detailed information is available in the paper.
