import os
import json
import csv
import sqlite3


def insert():
    try:
        sqliteConnection = sqlite3.connect('./db/data.db')
        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")
        location = ".\data"
        for file in os.listdir(location):
            if file.endswith('.json') or file.endswith('.JSON'):
                with open(f'{location}\\' + file, 'r') as j:
                    json_data = json.load(j)
                    for row in json_data:
                        cursor.execute('INSERT INTO invoices (InvoiceId, CustomerId, InvoiceDate, BillingAddress, '
                                       'BillingCity, BillingState, BillingCountry, BillingPostalCode, Total)'
                                       ' VALUES (?,?,?,?,?,?,?,?,?)'
                                       ,
                                       (row['InvoiceId'], row['CustomerId'], row['InvoiceDate'], row['BillingAddress'],
                                        row['BillingCity'], row['BillingState'], row['BillingCountry'],
                                        row['BillingPostalCode'], row['Total']))
                        sqliteConnection.commit()
                        print("Record inserted successfully into SqliteDb_developers table ", cursor.rowcount)
            if file.endswith('.csv') or file.endswith('.CSV'):
                with open(f'{location}\\' + file, 'r') as csv_file:
                    for row in csv.DictReader(csv_file):
                        cursor.execute('INSERT INTO invoices (InvoiceId, CustomerId, InvoiceDate, BillingAddress, '
                                       'BillingCity, BillingState, BillingCountry, BillingPostalCode, Total)'
                                       ' VALUES (?,?,?,?,?,?,?,?,?)'
                                       ,
                                       (row['InvoiceId'], row['CustomerId'], row['InvoiceDate'], row['BillingAddress'],
                                        row['BillingCity'], row['BillingState'], row['BillingCountry'],
                                        row['BillingPostalCode'], row['Total']))
                        sqliteConnection.commit()
                        print("Record inserted successfully into SqliteDb_developers table ", cursor.rowcount)

        cursor.close()
    except Exception as err:
        print(err)


if __name__ == '__main__':
    insert()
