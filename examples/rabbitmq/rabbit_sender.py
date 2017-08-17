#!/usr/bin/python

# RabbitMQ Sample (part 1): Message sender
# Your RabbitMQ service needs to be running first!

# This script generates 10 CSV-formatted messages and sends them to 'input' queue

import pika
from datetime import datetime


def send_message(message):
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=message)

queue_name = "input"
connection = pika.BlockingConnection(pika.ConnectionParameters(
               'localhost'))
channel = connection.channel()
channel.queue_declare(queue=queue_name)

for i in range(100):
    msg = "%s,Session%s,%i" % (datetime.strftime(datetime.now(), format="%Y-%m-%d %H:%M:%S"), i, i)
    send_message(msg)
    print(("Message %s sent" % msg))
connection.close()

