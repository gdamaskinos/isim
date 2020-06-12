"""
  Copyright (c) 2020 Georgios Damaskinos
  All rights reserved.
  @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
  This source code is licensed under the MIT license found in the
  LICENSE file in the root directory of this source tree.
"""

"""TCP-based communication"""

import argparse
import socket
import numpy as np
import sys
import json
import time

def openServerConn(port, hostname):
  """Waits for client to open connection and returns the connection"""
  sock = socket.socket()
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  sock.bind((hostname, port))

  sock.listen(5)

  # Wait for a connection
  print('Waiting for the frontend to connect...')
  connection, client_address = sock.accept()
  print('connection from', client_address)

  return connection

def openClientConn(port, hostname):
  """Connects to server and returns socket"""
  print("Frontend: Trying to connect to the backend...")
  while True:
    try:
      sock = socket.socket()
      sock.connect((hostname, port))
      return sock
    except Exception as e:
      #print(type(e).__name__, e)
      time.sleep(1)

def closeConnection(sock):
  #sock.shutdown(1)
  sock.close()

def sendMessage(sock, message, isJSON=False):
  """Sends a message to the connection (non-blocking)"""
  if not isJSON:
    try:
      message = json.dumps(list(map(lambda x: str(x), message)))
#      message = json.dumps(["1","2","3","4"]).encode()
    except json.JSONEncodeError:
      print("Input message cannot be parsed into array")

  message += "\n"
  #print("SEND: ", message)
  message = message.encode()
  sock.sendall(message)


def getMessage(connection, isJSON=False):
  """Blocks until receiving a message from the connection
  Returns:
    (np.array): message or None if exception (e.g., connection closed)
  """

  result = None
  try:
    data = b''
    BUFF_SIZE = 1024
    while True:
      part = connection.recv(BUFF_SIZE)
      data += part
      #print("NEXT", len(part), part.decode()[-1])
      if part.decode()[-1] == "\n":
       #if len(part) == 0: # < BUFF_SIZE:
        break

    result = data.decode()[:-1].encode()

    if not isJSON:
      try:
        result = np.array(json.loads(data))
        #print("GOT (size=%d): %s" % (len(result), result))
      except json.JSONDecodeError:
        print("Input message cannot be parsed into array")
        result = result.decode()
        print(result)

  except Exception as e:
      print("Failed to receive message: ", type(e).__name__, e)
  finally:
      return result


