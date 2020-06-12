"""
  Copyright (c) 2020 Georgios Damaskinos
  All rights reserved.
  @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
  This source code is licensed under the MIT license found in the
  LICENSE file in the root directory of this source tree.
"""

"""Parser for the wikiElec dataset on
https://snap.stanford.edu/data/wiki-Elec.html"""

import argparse
import pandas as pd
import time

def unixTimestamp(date):
    t = pd.Timestamp(date)
    return int(time.mktime(t.timetuple()))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='wikiElec parser:\
            !not sorted. Output: Uid,Vid,vote,timestamp')
    parser.add_argument("input",help="/path/to/wikiElec.ElecBs3.txt (see link above to download from)")
    parser.add_argument("output",help="The output filename (.csv)")

    args = parser.parse_args()

    values = []
    for line in open(args.input, encoding="ISO-8859-1"):
        if line[0] == 'U': # new user
            line = line.split("\t")
            uid = line[1]
        elif line[0] == 'V':
            line = line.split("\t")
            vid = line[2]
            vote = line[1]
            timestamp = line[3]
            values.append([uid, vid, vote, timestamp])

    df = pd.DataFrame(values, columns=('uid', 'vote_id', 'vote', 'timestamp'))
    df['timestamp'] = df['timestamp'].apply(unixTimestamp)
    sortedDF = df.sort_values(by=['timestamp'])
    sortedDF.to_csv(args.output, index=False, header=False)
