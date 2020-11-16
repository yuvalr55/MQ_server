from pika import URLParameters, BlockingConnection
from os import environ
from time import sleep
from queries import Query
from threading import Thread


def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread

    return wrapper


class Status:
    def __init__(self):
        self.data = []

    @threaded
    def main(self):
        url = environ.get('CLOUDAMQP_URL',
                          'amqps://ahsmnsum:ZvUDGHG1jM9zRGylIlSLBs1WPRwtucj5@woodpecker.rmq.cloudamqp.com/ahsmnsum')
        params = URLParameters(url)
        connection = BlockingConnection(params)
        channel = connection.channel()
        print('start')
        query = Query()
        query.connect()
        while True:
            inmessage = channel.basic_get("received", auto_ack=True)
            sleep(0.2)
            if inmessage[2] is not None:
                print('ok')
                while True:
                    self.data = query.select()
            sleep(0.8)

    def select(self):
        return self.data


status = Status()
status.main()

# if __name__ == '__main__':
#     try:
#         status = Status()
#         status.main()
#         select = status.select()
#         print(select)
#     except KeyboardInterrupt:
#         print('Interrupted')
#     try:
#         exit(0)
#     except SystemExit as err:
#         raise err
