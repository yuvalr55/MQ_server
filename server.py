import pika
import os

url = os.environ.get('CLOUDAMQP_URL',
                     'amqps://ahsmnsum:ZvUDGHG1jM9zRGylIlSLBs1WPRwtucj5@woodpecker.rmq.cloudamqp.com/ahsmnsum')
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')


def on_request(ch, method, props, body):
    print(body)
    response = body

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start():
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)
    print(" [x] Awaiting RPC requests")
    channel.start_consuming()


if __name__ == '__main__':
    start()
