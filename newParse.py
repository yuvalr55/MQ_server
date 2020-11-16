from os import path
from json import load
from csv import DictReader
from sqlite3 import connect
import pandas as pd
from datetime import datetime


class NewParse:

    def __init__(self, location, table_name, file_type):
        self.path = '.\db\data.db'  # Location of the DB
        self.location = location  # File path
        self.table_name = table_name  # The name of the table in DB
        self.file_type = file_type  # File type
        self.conn = None  # Connect to DB
        self.counter = 0
        self.dataForGraph = {'customers': {}, 'invoices': {}}
        self.quantity_of_customers = []

        self.location_and_type = f'{self.location}.{self.file_type}'

    def getPath(self):
        return self.path

    def connect(self):
        try:
            conn = connect(self.path)
            self.conn = conn
            return self.conn
        except Exception as err:
            print('connection error:', err)
            return -1

    def disconnect(self):
        if self.conn:
            self.conn.close()
            print("disconnected from database")

    def csv_parse_and_insert(self):
        try:
            cursor = self.conn.cursor()
            with open(f'{self.location_and_type}', 'r') as csv_file:
                for row in DictReader(csv_file):
                    InvoiceDate = str(pd.to_datetime(row['InvoiceDate'][:10]))
                    cursor.execute(
                        f'INSERT INTO {self.table_name} (InvoiceId, CustomerId, InvoiceDate, BillingAddress, '
                        'BillingCity, BillingState, BillingCountry, BillingPostalCode, Total)'
                        ' VALUES (?,?,?,?,?,?,?,?,?)'
                        ,
                        (row['InvoiceId'], int(row['CustomerId']), InvoiceDate,
                         row['BillingAddress'],
                         row['BillingCity'], row['BillingState'], row['BillingCountry'],
                         row['BillingPostalCode'], row['Total']))
                    NewParse.total(self, InvoiceDate)
                    NewParse.customers(self, row['CustomerId'], InvoiceDate)
                    self.counter += 1
                self.conn.commit()
            return f'{self.table_name} table: Query returned successfully: {self.counter} rows. {datetime.now()}'
        except Exception as err:
            return f'{self.table_name} table: "{err}" {self.counter} rows. {datetime.now()}'

    def json_parse_and_insert(self):
        try:
            cursor = self.conn.cursor()
            with open(f'{self.location_and_type}', 'r') as json_file:
                json_data = load(json_file)
                for row in json_data:
                    cursor.execute(
                        f'INSERT INTO {self.table_name} (InvoiceId, CustomerId, InvoiceDate, BillingAddress, '
                        'BillingCity, BillingState, BillingCountry, BillingPostalCode, Total)'
                        ' VALUES (?,?,?,?,?,?,?,?,?)'
                        ,
                        (row['InvoiceId'], row['CustomerId'], row['InvoiceDate'],
                         row['BillingAddress'],
                         row['BillingCity'], row['BillingState'], row['BillingCountry'],
                         row['BillingPostalCode'], row['Total']))
                    NewParse.total(self, row['InvoiceDate'])
                    NewParse.customers(self, row['CustomerId'], row['InvoiceDate'])
                    self.counter += 1
                self.conn.commit()
            return f'{self.table_name} table: Query returned successfully: {self.counter} rows. {datetime.now()}'
        except Exception as err:
            return f'{self.table_name} table: "{err}" {self.counter} rows. {datetime.now()}'

    def check_format_file(self):
        try:
            if path.isfile(self.location_and_type):
                if self.location_and_type.endswith('.json') or self.location_and_type.endswith('.JSON'):
                    return NewParse.json_parse_and_insert(self)
                elif self.location_and_type.endswith('.csv') or self.location_and_type.endswith('.CSV'):
                    return NewParse.csv_parse_and_insert(self)
            else:
                return 'The file does not exist'
        except Exception as err:
            return err

    def total(self, invoiceDate):
        if invoiceDate[:7] + invoiceDate[10:] in self.dataForGraph['invoices']:
            self.dataForGraph['invoices'][invoiceDate[:7] + invoiceDate[10:]] += 1
        else:
            self.quantity_of_customers = []
            self.dataForGraph['invoices'][invoiceDate[:7] + invoiceDate[10:]] = 1

    def customers(self, customerId, invoiceDate):
        self.quantity_of_customers.append(customerId)
        quantity_of_customers = list(dict.fromkeys(self.quantity_of_customers))
        self.dataForGraph['customers'][invoiceDate[:7] + invoiceDate[10:]] = len(quantity_of_customers)

    def insert_for_graph(self):
        try:
            cursor = self.conn.cursor()
            self.counter = 0
            for date_time in self.dataForGraph['customers'].keys():
                cursor.execute(
                    f'INSERT INTO graph (invoiceDate, totalSales, customers)'
                    ' VALUES (?,?,?)'
                    ,
                    (date_time, self.dataForGraph['invoices'][date_time], self.dataForGraph['customers'][date_time]))
                self.counter += 1
            self.conn.commit()
            return f'graph table: {self.counter} rows. {datetime.now()}'
        except Exception as err:
            return f'{self.table_name} table: "{err}" {self.counter} rows. {datetime.now()}'


# if __name__ == '__main__':
#     location, table_name, file_type = ('.\data_input\invoices_2009', 'invoices', 'json')
#     parse = NewParse(location, table_name, file_type)
#     parse.connect()
#     print(parse.check_format_file())
#     print(parse.insert_for_graph())
#     parse.disconnect()
