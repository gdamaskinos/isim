"""
  Copyright (c) 2020 Georgios Damaskinos
  All rights reserved.
  @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
  This source code is licensed under the MIT license found in the
  LICENSE file in the root directory of this source tree.
"""

"""Parser for the MovieLens dataset found on
http://files.grouplens.org/datasets/movielens/ml-100k.zip"""

from functools import partial
import sys
import csv
import getopt
import gzip
import json
import argparse
import sys
import pandas as pd
import numpy as np
import time

def unixTimestamp(date):
    t = pd.Timestamp(date)
    return int(time.mktime(t.timetuple()))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Parse csv dataset:\
            select columns + sort.')
    parser.add_argument("input",help="The input filename (.csv)")
    parser.add_argument("output",help="The output filename (.csv)")

    args = parser.parse_args()

    np.random.seed(42)

    df = pd.read_csv(args.input, header=None, sep=None, engine='python')
    df[3] = df[3].apply(int)
    sortedDF = df.sort_values(by=[3])
    sortedDF.to_csv(args.output, index=False, header=False)
