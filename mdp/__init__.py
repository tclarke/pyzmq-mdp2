# -*- coding: utf-8 -*-
"""
This package implements the `ØQ Majordomo Protocol` as specified under http://rfc.zeromq.org/spec:18
Terms used
----------
  worker
    process offering exactly one service in request/reply fashion.
  client
    independant process using a service in request/reply fashion.
  broker
    process routing messages from a client to a worker and back.
  worker id
    the  ØQ socket identity of the worker socket communicating with the broker.
"""

from client import MDPClient
from worker import MDPWorker
