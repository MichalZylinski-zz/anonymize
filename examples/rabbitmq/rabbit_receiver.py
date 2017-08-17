#!/usr/bin/python

# RabbitMQ Sample (part 2): Message receiver
# Your RabbitMQ service needs to be running first!

#This script should receive transformed messages from anonymize service, according to the rules defined in rabbitmq_schema.json

import pika, time

def get_message(ch, method, properties, body):
    print(body.decode('utf-8'))

connection_string = "amqp://localhost/"
parameters = pika.URLParameters(connection_string)

queue_name = "output"
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.basic_consume(get_message, queue=queue_name, no_ack=True)
print("Listening started...")
channel.start_consuming()
