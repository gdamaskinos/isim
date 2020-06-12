"""
  Copyright (c) 2020 Georgios Damaskinos
  All rights reserved.
  @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
  This source code is licensed under the MIT license found in the
  LICENSE file in the root directory of this source tree.
 """

"""SwIFT backend implementation on PySpark with Cassandra"""

import argparse
import pandas as pd
import numpy as np
from utils import comm

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='SwIFT frontend')
    parser.add_argument("--backend_port",
                        type=int,
                        default=9995,
                        help='Port for communicating with the backend')
    parser.add_argument("--backend_ip", type=str,
                        default="127.0.0.1",
                        help='Backend IP address')
    parser.add_argument("--test_set",
                        help="/path/to/testSet.csv containing rows: <userID,itemID,rating,timestamp>")
    parser.add_argument("--mbatch_size", type=int, default=1,
                        help="micro-batch size")

    # TODO implement evaluation for multiple topN values (see listR in the backend)

    args = parser.parse_args()

    data = pd.read_csv(args.test_set, header=None)
    data.columns = ['user', 'item', 'rating', 'unixtimestamp']

    mbatches = np.array_split(data, int(len(data) / args.mbatch_size))

    conn = comm.openClientConn(args.backend_port, args.backend_ip)

    recommended = {} # user_id -> recommended_items_list
    for mbatch in mbatches:
        print("Frontend: Sending micro-batch to backend...")
        comm.sendMessage(conn, mbatch.to_json(), isJSON=True)

        # get the updated recommendations
        while True:
            msg = comm.getMessage(conn)
            if list(msg) == ['A', 'C', 'K']:
                break
            msg = msg.astype(int)
            # first int is user and the rest are recoms
            user = msg[0]
            new_recom = list(msg[1:])
            if user not in recommended:
              recommended[user] = new_recom
            else:
              recommended[user] += new_recom

    comm.closeConnection(conn)

    # user_id -> clicked_items_list
    clicked = data.groupby('user')['item'].apply(list)

    # calculate CTR and recall per user
    total_relevant = 0
    total_recommended = 0
    total_ctr = 0
    total_recall = 0
    total_users = 0
    for user in clicked.keys():
      if user in recommended:
        relevant = len(list(set(clicked[user]) & set(recommended[user])))
        recommendations = len(set(recommended[user]))

        total_relevant += relevant
        total_recommended += recommendations
        total_users += 1

        total_recall += relevant / float(len(set(clicked[user])))
        total_ctr += relevant / float(recommendations)


    print("Total relevant items: %d" % total_relevant)
    print("Total recommended items: %d" % total_recommended)

    avg_ctr = float(total_ctr) / float(total_users)
    print("CTR: %g" % avg_ctr)

    avg_recall = float(total_recall) / float(total_users)
    print("Recall: %g" % avg_recall)
