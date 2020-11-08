import pika
import uuid
import os


class Client(object):

    def __init__(self, data):
        self.corr_id = str(uuid.uuid4())
        self.json_input = data
        self.response = None
        url = os.environ.get('CLOUDAMQP_URL',
                             'amqps://ahsmnsum:ZvUDGHG1jM9zRGylIlSLBs1WPRwtucj5@woodpecker.rmq.cloudamqp.com/ahsmnsum')
        params = pika.URLParameters(url)
        self.connection = pika.BlockingConnection(params)
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
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=self.json_input)
        while self.response is None:
            self.connection.process_data_events()
        return self.corr_id


# if __name__ == '__main__':
    # Test
    # import json
    # try:
    #     target_table = 'table01'
    #     location = ".\data"
    #     for file in os.listdir(location):
    #         if file.endswith('.json') or file.endswith('.JSON') or file.endswith('.csv') or file.endswith('.CSV'):
    #             file_extension = os.path.splitext(location + file)[1][1:]
    #             json_input = json.dumps(
    #                 {'target_table': target_table, 'location': location, 'type_file': file_extension})
    #             print(" [x] Requesting data from server")
    #             fibonacci_rpc = Client(json_input)
    #             response = fibonacci_rpc.call()
    #             print(f"response from server: {response}")
    # except Exception as err:
    #     print(err)
