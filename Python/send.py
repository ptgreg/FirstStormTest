#!/usr/bin/env python

import sys
sys.path.append('gen-py')
import pika
import random
import time
import SimpleStruct
from SimpleStruct.ttypes import *
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')


while True:
    rNb = random.randint(0,100)

    simpleStruct = SimpleStruct()
    if rNb % 2 == 0:
        simpleStruct.Provider = rNb
    else:
        simpleStruct.Merchant = rNb
    simpleStruct.Keyword = 'Kw %d' % rNb

    transportOut = TTransport.TMemoryBuffer()
    protocolOut = TBinaryProtocol.TBinaryProtocol(transportOut)
    simpleStruct.write(protocolOut)
    bytes = transportOut.getvalue()

    channel.basic_publish(exchange='',
                        routing_key='hello',
                        body=bytes)
    time.sleep(2)
    print "[x] Sent %d" % rNb

connection.close()