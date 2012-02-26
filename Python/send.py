#!/usr/bin/env python

import pika
import random
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')


while True:
    rNb = random.randint(0,100)
    channel.basic_publish(exchange='',
                        routing_key='hello',
                        body='%d' % rNb)
    #time.sleep(2)
    print "[x] Sent %d" % rNb

connection.close()