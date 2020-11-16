from pika import URLParameters, BasicProperties, BlockingConnection
from uuid import uuid4
from os import environ


class Client(object):

    def __init__(self, data):
        self.corr_id = str(uuid4())
        self.json_input = data
        self.response = None
        url = environ.get('CLOUDAMQP_URL',
                          'amqps://ahsmnsum:ZvUDGHG1jM9zRGylIlSLBs1WPRwtucj5@woodpecker.rmq.cloudamqp.com/ahsmnsum')
        params = URLParameters(url)
        self.connection = BlockingConnection(params)
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self):
        self.channel.basic_publish(
            exchange='',
            routing_key='send',
            properties=BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=self.json_input)
        while self.response is None:
            self.connection.process_data_events()
        return self.response


# if __name__ == '__main__':
#     from json import dumps
#
#     location = ".\data_input\invoices_2012"
#     type = 'csv'
#     table = 'invoices'
#     json_input = dumps({
#         "location": location,
#         "type": type,
#         "table": table
#     })
#     client = Client(json_input)
#     response = client.call()
#     print(response)
