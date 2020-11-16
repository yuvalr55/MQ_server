from sqlite3 import connect


class Query:
    flag_for_db = False

    def __init__(self):
        self.path = '.\db\data.db'
        self.conn = None

    def getPath(self):
        return self.path

    def connect(self):
        try:
            conn = connect(self.path)
            self.conn = conn
            return self.conn
        except:
            print('connection error')
            return -1

    def disconnect(self):
        if self.conn:
            self.conn.close()
            print("disconnected from database")

    def select(self):
        try:
            cur = self.conn.cursor()
            cur.execute('SELECT * from graph ORDER BY "InvoiceDate"')
            res = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
            return res

        except Exception as err:
            print(err)
            return []


# if __name__ == '__main__':
#     Query.status = False
#     dbHandler = Query()
#     dbHandler.connect()
#     data_from_db = dbHandler.select()
#     print(data_from_db)
