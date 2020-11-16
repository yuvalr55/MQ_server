from pika import BasicProperties, URLParameters, BlockingConnection
from os import environ
from ast import literal_eval
from newParse import NewParse


def on_request(ch, method, props, body):
    data_for_db = literal_eval(body.decode("utf-8"))
    parse = NewParse(data_for_db['location'], data_for_db['table'], data_for_db['type'])
    parse.connect()
    fun1 = parse.check_format_file()
    fun2 = parse.insert_for_graph()
    parse.disconnect()
    if 'Query returned successfully' in fun1:
        channel.queue_declare(queue='received')
        channel.basic_publish(exchange='', routing_key='received', body=f'ok')
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=BasicProperties(correlation_id=props.correlation_id),
                     body=str(f'{fun1} | {fun2}'))
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    url = environ.get('CLOUDAMQP_URL',
                      'amqps://ahsmnsum:ZvUDGHG1jM9zRGylIlSLBs1WPRwtucj5@woodpecker.rmq.cloudamqp.com/ahsmnsum')
    params = URLParameters(url)
    connection = BlockingConnection(params)

    channel = connection.channel()

    channel.queue_declare(queue='send')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='send', on_message_callback=on_request)

    print(" [x] Awaiting RPC requests")
    channel.start_consuming()
